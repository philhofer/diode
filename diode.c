#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>  /* ssize_t */
#include <fcntl.h>   /* fcntl, O_XXX */
#include <stdint.h>  /* size_t, uint32_t */
#include <pthread.h> /* pthread_xxx */
#include <assert.h>
#include <stdio.h> /* perror() */
#include <errno.h> /* errno */
#include <time.h>  /* struct timespec */

int epoll_create1(int);
typedef int64_t loff_t;
ssize_t splice();
int pipe2(int pipefd[2], int flags);

#define __SYSERR(FL, LN, FN) perror(#FL " " #LN FN)
#define SYSERR(FN) __SYSERR(__FILE__, __LINE__, FN)

/* special pointers for success and failure */
static uintptr_t success_ptr;

/* a unique per-request token */
typedef uint32_t iokey_t;

/* list of io operations */
typedef enum {
  IO_PWRITE,
  IO_PREAD,
  IO_FSYNC,
} iop_t;

typedef struct io_req io_req_t;

#define NUM_WORKERS 4

typedef struct {
  pthread_t workers[NUM_WORKERS]; /* worker threads */
  
  int w_fd; /* write end of a pipe */
  int r_fd; /* read end of a pipe */

  pthread_mutex_t lock; /* guards fields below */
  pthread_cond_t  cond; /* condition for queue emptyness */
  io_req_t *top;        /* top of linked list */
  io_req_t *tail;       /* tail of linked list */
  int       inqueue;    /* number of elements in queue */
} io_queue_t;


static void *worker_start(void *v);
static void perform_io(io_req_t *irq);
static io_req_t *queue_pop(io_queue_t *ioq);

/* queue_init
 * creates a pipe to communicate with
 * worker threads and launches a pool
 * of workers.
 */
static int queue_init(io_queue_t *ioq) {
  int pipefd[2];
  if (pipe2(pipefd, O_CLOEXEC) == -1) return -1;
  ioq->r_fd = pipefd[0];
  ioq->w_fd = pipefd[1];

  /* the read fd should be non-blocking;
   * it is consumed by the event loop.
   */
  int flags = fcntl(ioq->r_fd, F_GETFL);
  flags |= O_NONBLOCK;
  fcntl(ioq->r_fd, F_SETFL, flags);
  
  pthread_mutex_init(&ioq->lock, NULL);
  pthread_cond_init(&ioq->cond, NULL);

  ioq->top = NULL;
  ioq->tail = NULL;
  ioq->inqueue = 0;

  for (int i=0; i<NUM_WORKERS; ++i) {
    assert(pthread_create(&ioq->workers[i], NULL, worker_start, (void *)ioq) == 0);
  }

  return 0;
}

/* valid flags in io_req.flags */
#define IO_F_OPEN     1 /* open io_req.path and set it to io_req.fd */
#define IO_F_KEEPOPEN 2 /* don't close() after i/o completion */
#define IO_F_SYNC     4 /* call fsync() after i/o */

struct io_req {
  io_queue_t *parent;   /* parent queue */
  io_req_t   *next;     /* next in queue; only used by io_queue operations */
  int         status;   /* 0 for success, errno on error */
  
  int       pipe_rd;    /* read end of pipe (used for write ops) */
  int       pipe_wr;    /* write end of pipe (used for read ops) */

  char     *path;       /* target path, if fd == -1 (gets open()'d and close()'d) */
  int       fd;         /* target fd */
  int       flags;      /* IO_XXX flags */
  loff_t    off;        /* offset to read/write to/from */
  size_t    sz;         /* size of op (~0 effectively means until EOF) */
  iop_t     op;         /* op type */
};

static void *worker_start(void *v) {
  io_queue_t *ioq = (io_queue_t *)v;
  io_req_t *irq;
  while ((irq = queue_pop(ioq))) {
    perform_io(irq);
  }
  return &success_ptr;
}

static void queue_destroy(io_queue_t *ioq) {
  /* ioq->inqueue == -1 is the exit
   * signal for waiting workers.
   * in-progress i/o will still
   * be completed.
   */
  pthread_mutex_lock(&ioq->lock);
  ioq->inqueue = -1;
  pthread_mutex_unlock(&ioq->lock);
  pthread_cond_broadcast(&ioq->cond);

  void *status;
  for (int i=0; i<NUM_WORKERS; ++i) {
    assert(pthread_join(ioq->workers[i], &status));
    assert(status == (void *)success_ptr);
  }

  /* we can't destory these mutexes until
   * we're sure that the worker threads
   * have exited.
   */
  pthread_mutex_destroy(&ioq->lock);
  pthread_cond_destroy(&ioq->cond);
  
  return;
}

/* pop a request off of the queue 
 * (blocks until one is available, or
 * until the queue is closed.)
 */
static io_req_t *queue_pop(io_queue_t *ioq) {
  io_req_t *out = NULL;
  pthread_mutex_lock(&ioq->lock);
  while (ioq->inqueue == 0) {
    pthread_cond_wait(&ioq->cond, &ioq->lock);
  }
  if (ioq->inqueue == -1) {
    pthread_mutex_unlock(&ioq->lock);
    return out;
  }
  out = ioq->top;
  assert(out);
  if (ioq->top->next == NULL) {
    assert(ioq->top == ioq->tail);
    ioq->top = NULL;
    ioq->tail = NULL;
    ioq->inqueue--;
    assert(ioq->inqueue == 0);
  } else {
    ioq->top = ioq->top->next;
    ioq->inqueue--;
  }
  pthread_mutex_unlock(&ioq->lock);
  out->next = NULL;
  return out;
}

static void queue_pushback(io_queue_t *ioq, io_req_t *req) {
  req->next = NULL;
  pthread_mutex_lock(&ioq->lock);
  if (ioq->tail == NULL) {
    ioq->top = req;
    ioq->tail = req;
    ioq->inqueue++;
    assert(ioq->inqueue == 1);
    pthread_cond_signal(&ioq->cond);
  } else {
    ioq->tail->next = req;
    ioq->tail = req;
    ioq->inqueue++;
    assert(ioq->inqueue > 1);
  }
  pthread_mutex_unlock(&ioq->lock);
  return;
}

static int chk_open(io_req_t *irq) {
  if (irq->flags&IO_F_OPEN) {
    assert(irq->path);
    if ((irq->fd = open(irq->path, O_RDWR|O_CLOEXEC)) == -1) {
      return -1;
    }
  }
  return 0;
}

static int chk_sync(io_req_t *irq) {
  if (irq->flags&IO_F_SYNC) {
    return fsync(irq->fd);
  }
  return 0;
}

static int chk_close(io_req_t *irq) {
  if ((irq->flags&IO_F_OPEN) && !(irq->flags&IO_F_KEEPOPEN)) {
    return close(irq->fd);
  }
  return 0;
}

/* in order to make the return-value synchronization
 * of outstanding i/o requests more straightforward,
 * we just use a pipe between the i/o worker threads
 * and the main event loop -- the other end of the
 * pipe is part of the epoll set.
 */
static void notify_complete(io_req_t *irq) {
  assert(irq->parent);
  if (write(irq->parent->w_fd, &irq, sizeof(irq)) == -1) {
    /* we have no recourse but to log the error */
    SYSERR("write()");
  }
}

static int perform_write(io_req_t *irq);
static int perform_read(io_req_t *irq);
static int perform_fsync(io_req_t *irq);

static void perform_io(io_req_t *irq) {
  int status;
  if (chk_open(irq) == -1) {
    status = errno;
    goto exit;
  }
  
  switch (irq->op) {
  case IO_PWRITE:
    status = perform_write(irq);
    break;
  case IO_PREAD:
    status = perform_read(irq);
    break;
  case IO_FSYNC:
    status = perform_fsync(irq);
    break;
  default:
    status = ENOTSUP;
  }

  /* on success, check for sync() */
  if (!status) {
    int syncerr = chk_sync(irq);
    if (syncerr && !status) {
      status = syncerr;
    }
  }

  /* regardless of success, check close() */
  int closerr = chk_close(irq);
  if (closerr && !status) {
    status = closerr;
  }
  
 exit:
  irq->status = status;
  notify_complete(irq);
  return;
}

/* splice from the pipe (must be the read end)
 * to the target fd, which should support offsets.
 */
static int perform_write(io_req_t *irq) {
  while (irq->sz) {
    /* splice from pipe fd to file */
    ssize_t ok = splice(irq->pipe_rd, NULL, irq->fd, &irq->off, irq->sz, SPLICE_F_MOVE|SPLICE_F_MORE);
    if (ok == -1) {
      return errno;
    }
    if (ok == 0) {
      return 0;
    }
    irq->sz -= (size_t)ok;
    irq->off += (loff_t)ok;
  }  
  return 0;
}

static int perform_read(io_req_t *irq) {
  while (irq->sz) {
    /* splice from file to pipe fd */
    ssize_t ok = splice(irq->fd, &irq->off, irq->pipe_wr, NULL, irq->sz, SPLICE_F_MOVE|SPLICE_F_MORE);
    if (ok == -1) {
      return errno;
    }
    if (ok == 0) {
      return 0; /* TODO: handle unexpected EOF */
    }
    irq->sz -= (size_t)ok;
    irq->off += (loff_t)ok;
  }
  return 0;
}

static int perform_fsync(io_req_t *irq) {
  int status = fsync(irq->fd);
  if (status == -1) {
    return errno;
  }
  return 0;
}

/* state of io_ctx_t */
enum {
  IO_STATE_HDR,  /* reading request */
  IO_STATE_BODY, /* reading/writing body */
  IO_STATE_RESP  /* rdbuf contains response status */
};

typedef struct {
  io_req_t req;
  int      sock_fd;
  int      state;
  
  /* buffer used for reading 
   * the request header.
   */
  char     rdbuf[256];
  int      curs; /* buffer cursor */
  
} io_ctx_t;

/* slab of 128 io_ctx */
#define BITSLAB_TYPE_NAME ctxmm_t
#define BITSLAB_TYPE io_ctx_t
#define BITSLAB_FN_PREFIX ctx
#define BITSLAB_SIZE 128
#include "bitslab.h"

static ctxmm_t ctx_heap;

static io_ctx_t *get_ctx(void) {
  return ctx_malloc(&ctx_heap);
}

static void free_ctx(io_ctx_t *ctx) {
  ctx_free(&ctx_heap, ctx);
  return;
}

static int epollfd; /* epoll fd */
static io_queue_t queue; /* thread pool */
static int lsock; /* listen socket */

static void init(void) {
  __builtin_memset(&ctx_heap, 0, sizeof(ctx_heap));
  epollfd = epoll_create1(EPOLL_CLOEXEC);
  if (epollfd == -1) {
    perror("epoll");
    _exit(1);
  }

  if (queue_init(&queue) == -1) {
    perror("queue init");
    _exit(1);
  }

  for (int i=0; i<BITSLAB_SIZE; ++i) {
    int pipefd[2];
    io_ctx_t *ctx = &ctx_heap.mem[i];
    if (pipe2(pipefd, O_CLOEXEC) == -1) {
      perror("cloexec");
      _exit(1);
    }
    ctx->req.parent = &queue;
    ctx->req.pipe_rd = pipefd[0];
    ctx->req.pipe_wr = pipefd[1];
  }

  /* TODO: set up socket */
  /* if ((lsock = socket(AF_INET6, SOCK_STREAM|SOCK_NONBLOCK|SOCK_CLOEXEC, 0)) == -1) {
   *  perror("socket");
   * _exit(1);
   * }
   */

   /* add listener socket to epoll  */
   struct epoll_event ev;
   ev.events = EPOLLIN | EPOLLHUP | EPOLLERR  | EPOLLET;
   ev.data.ptr = NULL;
   if (epoll_ctl(epollfd, EPOLL_CTL_ADD, lsock, &ev) == -1) {
    perror("epoll_ctl");
    _exit(1);
   }
   
   /* add io completion pipe to epoll */
   ev.data.ptr = (void *)queue;
   if (epoll_ctl(epollfd, EPOLL_CTL_ADD, queue.r_fd, &ev) == -1) {
    perror("epoll_ctl");
    _exit(1);
   }

  return;
}

static void uninit(void) {
  queue_destroy(&queue);
  close(epollfd);
}

enum {
  REASON_IO_COMPLETE, /* i/o is complete, try to finish up */
  REASON_COPY_READY   /* socket/pipe is available for i/o */
};

static void ready(io_ctx_t *ctx, int reason, uint32_t events) {
  switch (reason) {
  case REASON_IO_COMPLETE:
    if (events&EPOLLOUT) {
      try_sock_write(ctx);
    }
    break;
  case REASON_COPY_READY:
    if (events&EPOLLOUT) {
      try_sock_write(ctx);
    }
    if (events&EPOLLIN) {
      try_sock_read(ctx);
    }
    break;
  default:
    assert(false);
  }
  return;
}

int main(void) {
  init();

  for (;;) {
    struct epoll_event evlist[64];
    int nev = epoll_wait(epollfd, evlist, 64, -1);
    if (nev == -1) {
      perror("epoll_wait");
      _exit(1);
    }

  ploop:
    for (int i=0; i < nev; ++i) {
      struct epoll_event ev = evlist[i];
      
      if (ev.data.ptr == NULL) {
	/* listener event */
	/* TODO */
	
      } else if (ev.data.ptr == &queue) {
	/* i/o completion */

	io_ctx_t *ctx;
	switch (read(queue.r_fd, &ctx, sizeof(ctx))) {
	case sizeof(ctx):
	  ready(ctx, REASON_IO_COMPLETE, ev.events);
	case -1:
	  if errno == EAGAIN {
	      continue ploop
	    }
	default:
	  assert(false && "strange read");
	}
	
      } else {
	/* socket or pipe r/w availability */

	io_ctx_t *ctx = (io_ctx_t *)ev.data.ptr;
	ready(ctx, REASON_COPY_READY, ev.events);
	
      }
      
    }
    
  }
  
  uninit();
}
