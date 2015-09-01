#include <stdint.h>
#include <stddef.h>

#define WORDSIZE sizeof(uintptr_t)
#define WORDBITS (WORDSIZE*8)

#ifndef BITSLAB_TYPE
#error "BITSLAB_TYPE must be defined"
#endif

#ifndef BITSLAB_TYPE_NAME
#error "BITSLAB_TYPE_NAME must be defined"
#endif

#ifndef BITSLAB_SIZE
#error "BITSLAB_SIZE must be defined"
#endif

#ifndef BITSLAB_FN_PREFIX
#error "BITSLAB_FN_PREFIX must be defined"
#endif

#define CONCAT(X,Y) X ##_## Y
#define PPCAT(X, Y) CONCAT(X, Y)

#define FN_NAME(NAME) PPCAT(BITSLAB_FN_PREFIX, NAME)

#define BITMAP_WORDS (BITSLAB_SIZE/WORDBITS)

#define STATIC_ASSERT(COND) typedef char static_assertion##__LINE__[(!!(COND))*2-1]

typedef struct {
  uintptr_t    bits[BITMAP_WORDS];
  BITSLAB_TYPE mem[BITSLAB_SIZE];
} BITSLAB_TYPE_NAME;

/* bitslab_init
 * zeroes the bitmap bits
 * (the contents of the slab are still undefined)
 */
void FN_NAME(init)(BITSLAB_TYPE_NAME *bs) {
  __builtin_memset(bs->bits, 0, sizeof(uintptr_t[BITMAP_WORDS]));
  return;
}

#ifdef TEST
#include <stdio.h>
#include <assert.h>
/* for testing, a way to dump bitset state */
void FN_NAME(dump)(BITSLAB_TYPE_NAME *bs) {
  printf("BITMAP STATE: (%lu words)\n", BITMAP_WORDS);
  for (unsigned int i=0; i<BITMAP_WORDS; ++i) {
    printf("word %d: 0x%lx\n", i, bs->bits[i]);
  }
  return;
}
#endif

/* bitslab_used
 * returns the number of elements
 * that are used.
 */
size_t FN_NAME(used)(BITSLAB_TYPE_NAME *bs) {
  size_t total = 0;
  for (unsigned int i=0; i<BITMAP_WORDS; ++i) {
    total += __builtin_popcountl(bs->bits[i]);
  }
  return total;
}

/* bitslab_avail
 * returns the number of free elements
 */
size_t FN_NAME(avail)(BITSLAB_TYPE_NAME *bs) {
  return BITSLAB_SIZE-FN_NAME(used)(bs);
}

/* we're using __builtin_clzl(unsigned long)
 * and __builtin_popcountl(unsigned long)
 * under the assumption that sizeof(ulong)==sizeof(uintptr)
 */
STATIC_ASSERT(sizeof(unsigned long) == sizeof(uintptr_t));

BITSLAB_TYPE *FN_NAME(malloc)(BITSLAB_TYPE_NAME *bs) {
  int bit;
  uintptr_t set;
#ifdef TEST
  size_t started = bitslab_avail(bs);
#endif
  for (unsigned int i=0; i<BITMAP_WORDS; ++i) {
    set = ~(bs->bits[i]);
    if (set == 0) continue;
    bit = (uintptr_t)__builtin_ffsl(set)-1;
    bs->bits[i] |= ((uintptr_t)1 << bit);
    /* return nth element, where n = 64*i+bit */
    uintptr_t addr = (uintptr_t)(bs->mem) + (uintptr_t)(sizeof(BITSLAB_TYPE) * ((i*WORDBITS)+bit));
#ifdef TEST
    assert(started-1 == bitslab_avail(bs));
#endif
    return (BITSLAB_TYPE *)addr;
  }
  return NULL;
}

void FN_NAME(free)(BITSLAB_TYPE_NAME *bs, BITSLAB_TYPE *mem) {
  uintptr_t addr = (uintptr_t)mem;
#ifdef TEST
  assert(addr >= (uintptr_t)bs->mem);
  assert(addr < (uintptr_t)bs->mem + (sizeof(BITSLAB_TYPE)*BITSLAB_SIZE));
#endif
  size_t offset = addr - (uintptr_t)bs->mem;
  size_t index = offset / sizeof(BITSLAB_TYPE);
  uintptr_t bit = index%WORDBITS;
  unsigned int word = index/WORDBITS;

  bs->bits[word] &= ~((uintptr_t)1 << bit);
  return;
}

#undef CONCAT
#undef PPCAT
#undef FN_NAME
#undef BITMAP_WORDS
#undef STATIC_ASSERT
#undef WORDSIZE
#undef WORDBITS
