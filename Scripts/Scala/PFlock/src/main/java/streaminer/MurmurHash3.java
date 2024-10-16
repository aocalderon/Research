/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package streaminer;


import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * MurmurHash3 implementation in Java, based on Austin Appleby's <a href=
 * "https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp"
 * >original in C</a>
 * 
 * Only implementing x64 version, because this should always be faster on 64 bit
 * native processors, even 64 bit being ran with a 32 bit OS; this should also
 * be as fast or faster than the x86 version on some modern 32 bit processors.
 * 
 * @author Patrick McFarland
 * @see <a href="http://sites.google.com/site/murmurhash/">MurmurHash website</a>
 * @see <a href="http://en.wikipedia.org/wiki/MurmurHash">MurmurHash entry on Wikipedia</a>
 * @since 5.0
 */

public class MurmurHash3 extends Hash {
   private static final Charset UTF8 = Charset.forName("UTF-8");
   
   private static MurmurHash3 _instance = new MurmurHash3();
  
    public static Hash getInstance() {
      return _instance;
    }

   static class State {
       // need to recompile...
      long h1;
      long h2;

      long k1;
      long k2;

      long c1;
      long c2;
   }

   static class StateInt {
      int h1;
      int h2;

      int k1;
      int k2;

      int c1;
      int c2;
   }
   static long getblock(byte[] key, int i) {
      return
           (((long) key[i + 0] & 0x00000000000000FFL) << 0)
         | (((long) key[i + 1] & 0x00000000000000FFL) << 8)
         | (((long) key[i + 2] & 0x00000000000000FFL) << 16)
         | (((long) key[i + 3] & 0x00000000000000FFL) << 24)
         | (((long) key[i + 4] & 0x00000000000000FFL) << 32)
         | (((long) key[i + 5] & 0x00000000000000FFL) << 40)
         | (((long) key[i + 6] & 0x00000000000000FFL) << 48)
         | (((long) key[i + 7] & 0x00000000000000FFL) << 56);
   }

   static int getblockInt(int[] key, int i) {
      return
              (((int) key[i + 0] & 0x000000FF) << 0)
                      | (((int) key[i + 1] & 0x000000FF) << 8)
                      | (((int) key[i + 2] & 0x000000FF) << 16)
                      | (((int) key[i + 3] & 0x000000FF) << 24)
                      | (((int) key[i + 4] & 0x000000FF) << 32)
                      | (((int) key[i + 5] & 0x000000FF) << 40)
                      | (((int) key[i + 6] & 0x000000FF) << 48)
                      | (((int) key[i + 7] & 0x000000FF) << 56);
   }


   static void bmix(State state) {
//      k1 *= c1;
//      k1  = ROTL64(k1,31);
//      k1 *= c2;
      state.k1 *= state.c1;
      state.k1  = (state.k1 << 31) | (state.k1 >>> 64 - 31);
      state.k1 *= state.c2;

//      h1 ^= k1;
//      h1  = ROTL64(h1,27);
//      h1 += h2;
//      h1  = h1 * 5 + 0x52dce729;
      state.h1 ^= state.k1;
      state.h1  = (state.h1 << 27) | (state.h1 >>> 64 - 27);
      state.h1 += state.h2;
      state.h1  = state.h1 * 5 + 0x52dce729;

//      k2 *= c2;
//      k2  = ROTL64(k2,33);
//      k2 *= c1;
      state.k2 *= state.c2;
      state.k2 = (state.k2 << 33) | (state.k2 >>> 64 - 33);
      state.k2 *= state.c1;

//      h2 ^= k2;
//      h2  = ROTL64(h2,31);
//      h2 += h1;
//      h2  = h2 * 5 + 0x38495ab5;
      state.h2 ^= state.k2;
      state.h2  = (state.h2 << 31) | (state.h2 >>> 64 - 31);
      state.h2 += state.h1;
      state.h2  = state.h2 * 5 + 0x38495ab5;

      //state.c1 = state.c1 * 5 + 0x7b7d159c;
      //state.c2 = state.c2 * 5 + 0x6bce6396;
   }

   static void bmixInt(StateInt state) {
      state.k1 *= state.c1;
      state.k1 = (state.k1 << 23) | (state.k1 >>> 64 - 23);
      state.k1 *= state.c2;
      state.h1 ^= state.k1;
      state.h1 += state.h2;

      state.h2 = (state.h2 << 41) | (state.h2 >>> 64 - 41);

      state.k2 *= state.c2;
      state.k2 = (state.k2 << 23) | (state.k2 >>> 64 - 23);
      state.k2 *= state.c1;
      state.h2 ^= state.k2;
      state.h2 += state.h1;

      state.h1 = state.h1 * 3 + 0x52dce729;
      state.h2 = state.h2 * 3 + 0x38495ab5;

      state.c1 = state.c1 * 5 + 0x7b7d159c;
      state.c2 = state.c2 * 5 + 0x6bce6396;
   }
   static long fmix(long k) {
      k ^= k >>> 33;
      k *= 0xff51afd7ed558ccdL;
      k ^= k >>> 33;
      k *= 0xc4ceb9fe1a85ec53L;
      k ^= k >>> 33;

      return k;
   }

   static int fmixInt(int k) {
      k ^= k >>> 33;
      k *= 0xed558ccd;
      k ^= k >>> 33;
      k *= 0x1a85ec53;
      k ^= k >>> 33;

      return k;
   }

   /**
    * Hash a value using the x64 128 bit variant of MurmurHash3
    *
    * @param a value to hash
    * @param seed random value
    * @return 128 bit hashed key, in an array containing two longs
    */
   public static long hashInt2(final int a, final int seed) {
      final long[] key = {a};
      State state = new State();
      //state.h1 = 0x9368e53c2f6af274L ^ seed;
      //state.h2 = 0x586dcd208f7cd3fdL ^ seed;
      state.h1 = seed;
      state.h2 = seed;
               //0x87c37b91114253d5
      state.c1 = 0x47c37b91114253d5L;
      //0x4cf5ad432745937f
      state.c2 = 0x4cf5ad432745937fL;

      //----------
      // body
      //System.out.println(state.h1 + "\t" + state.h2  + "\t" + state.c1  + "\t" + state.c2);

      for (int i = 0; i < key.length / 2; i++) {
         // shouldn't enter here...
         state.k1 = key[i * 2];
         state.k2 = key[i * 2 + 1];

         bmix(state);
      }

      //----------
      // tail
      //System.out.println(state.h1 + "\t" + state.h2  + "\t" + state.c1  + "\t" + state.c2);

      long tail = key[key.length - 1];

      if (key.length % 2 != 0) {
         state.k1 ^= tail;
         bmix(state);
      }

      //----------
      // finalization
      //System.out.println(state.h1 + "\t" + state.h2  + "\t" + state.c1  + "\t" + state.c2);

      state.h1 ^= key.length;
      state.h2 ^= key.length;
      //System.out.println(state.h1 + "\t" + state.h2  + "\t" + state.c1  + "\t" + state.c2);

      state.h1 += state.h2;
      state.h2 += state.h1;

      state.h1 = fmix(state.h1);
      state.h2 = fmix(state.h2);

      state.h1 += state.h2;
      state.h2 += state.h1;

      return state.h1;
   }

   /**
    * Hash a value using the x64 128 bit variant of MurmurHash3
    * 
    * @param key value to hash
    * @param seed random value
    * @return 128 bit hashed key, in an array containing two longs
    */
   public static long[] MurmurHash3_x64_128(final byte[] key, final int seed) {
      State state = new State();
      //state.h1 = 0x9368e53c2f6af274L ^ seed;
      //state.h2 = 0x586dcd208f7cd3fdL ^ seed;
      state.h1 = seed;
      state.h2 = seed;
               //0x87c37b91114253d5
      state.c1 = 0x87c37b91114253d5L;
               //0x4cf5ad432745937f
      state.c2 = 0x4cf5ad432745937fL;

      //----------
      // body
      for (int i = 0; i < key.length / 16; i++) {
         state.k1 = getblock(key, (i * 2 + 0) * 8);
         state.k2 = getblock(key, (i * 2 + 1) * 8);

         bmix(state);
      }

      //----------
      // tail
      int tail = (key.length >>> 4) << 4;

      state.k1 = 0;
      state.k2 = 0;

      switch (key.length & 15) {
         case 15: state.k2 ^= (long) key[tail + 14] << 48;
         case 14: state.k2 ^= (long) key[tail + 13] << 40;
         case 13: state.k2 ^= (long) key[tail + 12] << 32;
         case 12: state.k2 ^= (long) key[tail + 11] << 24;
         case 11: state.k2 ^= (long) key[tail + 10] << 16;
         case 10: state.k2 ^= (long) key[tail + 9] << 8;
         case 9:  state.k2 ^= (long) key[tail + 8] << 0;
            state.k2 *= state.c2;
            state.k2 = (state.k2 << 33) | (state.k2 >>> 64 - 33);
            state.k2 *= state.c1;
            state.h2 ^= state.k2;

         case 8:  state.k1 ^= (long) key[tail + 7] << 56;
         case 7:  state.k1 ^= (long) key[tail + 6] << 48;
         case 6:  state.k1 ^= (long) key[tail + 5] << 40;
         case 5:  state.k1 ^= (long) key[tail + 4] << 32;
         case 4:  state.k1 ^= (long) key[tail + 3] << 24;
         case 3:  state.k1 ^= (long) key[tail + 2] << 16;
         case 2:  state.k1 ^= (long) key[tail + 1] << 8;
         case 1:  state.k1 ^= (long) key[tail + 0] << 0;
            state.k1 *= state.c1;
            state.k1  = (state.k1 << 31) | (state.k1 >>> 64 - 31);
            state.k1 *= state.c2;
            state.h1 ^= state.k1;
            //bmix(state);
      }

      //----------
      // finalization
      state.h1 ^= key.length;
      state.h2 ^= key.length;

      state.h1 += state.h2;
      state.h2 += state.h1;

      state.h1 = fmix(state.h1);
      state.h2 = fmix(state.h2);

      state.h1 += state.h2;
      state.h2 += state.h1;

      return new long[] { state.h1, state.h2 };
   }

   public static int hashInt(int src, final int seed) {
      final int[] key = { src };
      StateInt state = new StateInt();

      state.h1 = 0x9368e53c ^ seed;
      state.h2 = 0x586dcd20 ^ seed;

      state.c1 = 0x87c37b91;
      state.c2 = 0x4cf5ad43;

      for (int i = 0; i < key.length / 16; i++) {
         state.k1 = getblockInt(key, i * 2 * 8);
         state.k2 = getblockInt(key, (i * 2 + 1) * 8);

         bmixInt(state);
      }

      state.k1 = 0;
      state.k2 = 0;

      int tail = (key.length >>> 4) << 4;

      switch (key.length & 15) {
         case 15: state.k2 ^= (long) key[tail + 14] << 48;
         case 14: state.k2 ^= (long) key[tail + 13] << 40;
         case 13: state.k2 ^= (long) key[tail + 12] << 32;
         case 12: state.k2 ^= (long) key[tail + 11] << 24;
         case 11: state.k2 ^= (long) key[tail + 10] << 16;
         case 10: state.k2 ^= (long) key[tail + 9] << 8;
         case 9:  state.k2 ^= (long) key[tail + 8] << 0;
         case 8:  state.k1 ^= (long) key[tail + 7] << 56;
         case 7:  state.k1 ^= (long) key[tail + 6] << 48;
         case 6:  state.k1 ^= (long) key[tail + 5] << 40;
         case 5:  state.k1 ^= (long) key[tail + 4] << 32;
         case 4:  state.k1 ^= (long) key[tail + 3] << 24;
         case 3:  state.k1 ^= (long) key[tail + 2] << 16;
         case 2:  state.k1 ^= (long) key[tail + 1] << 8;
         case 1:  state.k1 ^= (long) key[tail + 0] << 0;
         bmixInt(state);
      }

      state.h2 ^= key.length;

      state.h1 += state.h2;
      state.h2 += state.h1;

      state.h1 = fmixInt(state.h1);
      state.h2 = fmixInt(state.h2);

      state.h1 += state.h2;
      state.h2 += state.h1;

      return state.h1;
   }

   public static int[] MurmurHash3_x64_128_int(final int[] key, final int seed) {
      StateInt state = new StateInt();

      state.h1 = 0x9368e53c ^ seed;
      state.h2 = 0x586dcd20 ^ seed;

      state.c1 = 0x87c37b91;
      state.c2 = 0x4cf5ad43;

      for (int i = 0; i < key.length / 16; i++) {
         state.k1 = getblockInt(key, i * 2 * 8);
         state.k2 = getblockInt(key, (i * 2 + 1) * 8);

         bmixInt(state);
      }

      state.k1 = 0;
      state.k2 = 0;

      int tail = (key.length >>> 4) << 4;

      switch (key.length & 15) {
         case 15: state.k2 ^= (long) key[tail + 14] << 48;
         case 14: state.k2 ^= (long) key[tail + 13] << 40;
         case 13: state.k2 ^= (long) key[tail + 12] << 32;
         case 12: state.k2 ^= (long) key[tail + 11] << 24;
         case 11: state.k2 ^= (long) key[tail + 10] << 16;
         case 10: state.k2 ^= (long) key[tail + 9] << 8;
         case 9:  state.k2 ^= (long) key[tail + 8] << 0;

         case 8:  state.k1 ^= (long) key[tail + 7] << 56;
         case 7:  state.k1 ^= (long) key[tail + 6] << 48;
         case 6:  state.k1 ^= (long) key[tail + 5] << 40;
         case 5:  state.k1 ^= (long) key[tail + 4] << 32;
         case 4:  state.k1 ^= (long) key[tail + 3] << 24;
         case 3:  state.k1 ^= (long) key[tail + 2] << 16;
         case 2:  state.k1 ^= (long) key[tail + 1] << 8;
         case 1:  state.k1 ^= (long) key[tail + 0] << 0;
         bmixInt(state);
      }

      state.h2 ^= key.length;

      state.h1 += state.h2;
      state.h2 += state.h1;

      state.h1 = fmixInt(state.h1);
      state.h2 = fmixInt(state.h2);

      state.h1 += state.h2;
      state.h2 += state.h1;

      return new int[] { state.h1, state.h2 };
   }

   /**
    * Hash a value using the x64 64 bit variant of MurmurHash3
    * 
    * @param key value to hash
    * @param seed random value
    * @return 64 bit hashed key
    */
   public static long MurmurHash3_x64_64(final byte[] key, final int seed) {
      return MurmurHash3_x64_128(key, seed)[0];
   }

   /**
    * Hash a value using the x64 32 bit variant of MurmurHash3
    * 
    * @param key value to hash
    * @param seed random value
    * @return 32 bit hashed key
    */
   public static int MurmurHash3_x64_32(final byte[] key, final int seed) {
      return (int) (MurmurHash3_x64_128(key, seed)[0] >>> 32);
   }
   
   public long[] hash64(Object o, int seed) {
       long[] result;
      if (o instanceof byte[]) {
          result = MurmurHash3_x64_128((byte[]) o, seed);
      } else if (o instanceof long[]) {
          result = MurmurHash3_x64_128((long[]) o, seed);
      } else if (o instanceof String) {
          result = MurmurHash3_x64_128(((String) o).getBytes(UTF8), seed);
      } else {
          result = MurmurHash3_x64_128(((Integer)o.hashCode()).toString().getBytes(UTF8), seed);
      }
      
      return Arrays.copyOfRange(result, 0, 2);
   }

   /**
    * Hash a value using the x64 128 bit variant of MurmurHash3
    * 
    * @param key value to hash
    * @param seed random value
    * @return 128 bit hashed key, in an array containing two longs
    */
   public static long[] MurmurHash3_x64_128(final long[] key, final int seed) {
      State state = new State();

      state.h1 = 0x9368e53c2f6af274L ^ seed;
      state.h2 = 0x586dcd208f7cd3fdL ^ seed;

      state.c1 = 0x87c37b91114253d5L;
      state.c2 = 0x4cf5ad432745937fL;

      for (int i = 0; i < key.length / 2; i++) {
         state.k1 = key[i * 2];
         state.k2 = key[i * 2 + 1];

         bmix(state);
      }

      long tail = key[key.length - 1];

      if (key.length % 2 != 0) {
         state.k1 ^= tail;
         bmix(state);
      }

      state.h2 ^= key.length * 8;

      state.h1 += state.h2;
      state.h2 += state.h1;

      state.h1 = fmix(state.h1);
      state.h2 = fmix(state.h2);

      state.h1 += state.h2;
      state.h2 += state.h1;

      return new long[] { state.h1, state.h2 };
   }

   /**
    * Hash a value using the x64 64 bit variant of MurmurHash3
    * 
    * @param key value to hash
    * @param seed random value
    * @return 64 bit hashed key
    */
   public static long MurmurHash3_x64_64(final long[] key, final int seed) {
      return MurmurHash3_x64_128(key, seed)[0];
   }

   /**
    * Hash a value using the x64 32 bit variant of MurmurHash3
    * 
    * @param key value to hash
    * @param seed random value
    * @return 32 bit hashed key
    */
   public static int MurmurHash3_x64_32(final long[] key, final int seed) {
      return (int) (MurmurHash3_x64_128(key, seed)[0] >>> 32);
   }

   @Override
   public int hash(byte[] payload) {
      return MurmurHash3_x64_32(payload, 9001);
   }

   /**
    * Hashes a byte array efficiently.
    * 
    * @param payload a byte array to hash
    * @return a hash code for the byte array
    */
   public int hash(long[] payload) {
      return MurmurHash3_x64_32(payload, 9001);
   }

   public int hash(int hashcode) {
      byte[] b = new byte[4];
      b[0] = (byte) hashcode;
      b[1] = (byte) (hashcode >>> 8);
      b[2] = (byte) (hashcode >>> 16);
      b[3] = (byte) (hashcode >>> 24);
      return hash(b);
   }

   public int hash(Object o) {
      if (o instanceof byte[])
         return hash((byte[]) o);
      else if (o instanceof long[])
         return hash((long[]) o);
      else if (o instanceof String)
         return hash(((String) o).getBytes(UTF8));
      else
         return hash(o.hashCode());
   }
   
   @Override
    public int hash(byte[] bytes, int length, int initval) {
        return MurmurHash3_x64_32(bytes, initval);
    }

    @Override
    public long hash64(Object o) {
        return hash64(o, 9001)[0];
    }
    
    @Override
    public long hash64(byte[] bytes, int length, int seed) {
        return hash64(bytes, seed);
    }
}