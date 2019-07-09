package javautils.encrypt;

/**
 * protect the password user inputed
 */
public class SHACode {
	private static final long SH0 = 0x67452301L;
	private static final long SH1 = 0xefcdab89L;
	private static final long SH2 = 0x98badcfeL;
	private static final long SH3 = 0x10325476L;
	private static final long SH4 = 0xc3d2e1f0L;

	private static final long K0 = 0x5a827999L;
	private static final long K1 = 0x6ed9eba1L;
	private static final long K2 = 0x8f1bbcdcL;
	private static final long K3 = 0xca62c1d6L;

	// df debug 12.11
	private static long[] W;
	private static byte[] BW;

	private static int p0, p1, p2, p3, p4;
	private static long A, B, C, D, E, temp;

	private static long f0(long x, long y, long z) {
		return (z ^ (x & (y ^ z)));
	}

	private static long f1(long x, long y, long z) {
		return (x ^ y ^ z);
	}

	private static long f2(long x, long y, long z) {
		return ((x & y) | (z & (x | y)));
	}

	private static long f3(long x, long y, long z) {
		return (x ^ y ^ z);
	}

	private static long S(int n, long X) {
		long tem = 0;
		X = X & 0x00000000FFFFFFFF;

		if (n == 1) {
			tem = ((X >> 31) & 0x00000001);

		}
		if (n == 5)
			tem = ((X >> 27) & 0x0000001F);
		if (n == 30)
			tem = ((X >> 2) & 0x3FFFFFFF);

		return ((X << n) | tem);

	}

	private static void r0(int f, long K) {

		temp = S(5, A) + f0(B, C, D) + E + W[p0++] + K;
		E = D;
		D = C;
		C = S(30, B);
		B = A;
		A = temp;
	}

	private static void r1(int f, long K) {

		temp = (W[p1++] ^ W[p2++] ^ W[p3++] ^ W[p4++]);

		switch (f) {
		case 0:
			temp = S(5, A) + f0(B, C, D) + E + (W[p0++] = S(1, temp)) + K;
			break;
		case 1:
			temp = S(5, A) + f1(B, C, D) + E + (W[p0++] = S(1, temp)) + K;
			break;
		case 2:
			temp = S(5, A) + f2(B, C, D) + E + (W[p0++] = S(1, temp)) + K;
			break;
		case 3:
			temp = S(5, A) + f3(B, C, D) + E + (W[p0++] = S(1, temp)) + K;
			break;
		}

		E = D;
		D = C;
		C = S(30, B);
		B = A;
		A = temp;
	}

	public static long getCode(String mem) {
		int i, nread, nbits;
		int length = mem.toCharArray().length;
		int hi_length, lo_length;
		int padded;
		char[] s;
		int sp = 0;
		long h0, h1, h2, h3, h4;
		W = new long[80];
		BW = new byte[320];
		padded = 0;
		s = mem.toCharArray();
		h0 = SH0;
		h1 = SH1;
		h2 = SH2;
		h3 = SH3;
		h4 = SH4;
		for (hi_length = lo_length = 0;;) {
			if (length < 64)
				nread = length;
			else
				nread = 64;
			length -= nread;
			for (int m = 0; m < nread; m++)
				BW[m] = (byte) s[sp++];
			if (nread < 64) {
				nbits = nread * 8;

				if ((lo_length += nbits) < (long) nbits)
					hi_length++;
				if ((nread < 64) && (padded == 0)) {
					BW[nread++] = (byte) 0x080;
					padded = 1;
				}

				for (i = nread; i < 64; i++)
					BW[i] = 0;

				byte[] tar = new byte[4];
				for (int z = 0; z < 64; z = z + 4) {
					for (int y = 0; y < 4; y++)
						tar[y] = BW[z + y];

					W[z >> 2] = 0;
					W[z >> 2] = (W[z >> 2] | tar[0]) << 8;
					W[z >> 2] = (W[z >> 2] | (tar[1] & 0x00FF)) << 8;
					W[z >> 2] = ((W[z >> 2] | (tar[2] & 0x00FF)) << 8)
							| (tar[3] & 0x00FF);
				}

				if (nread <= 56) {
					W[14] = hi_length;
					W[15] = lo_length;
				}
			} else {
				if ((lo_length += 512) < 512)
					hi_length++;

				byte[] tar = new byte[4];
				for (int z = 0; z < 64; z = z + 4) {
					for (int y = 0; y < 4; y++)
						tar[y] = BW[z + y];

					W[z >> 2] = 0;
					W[z >> 2] = (W[z >> 4] | tar[0]) << 8;
					W[z >> 2] = (W[z >> 2] | (tar[1] & 0x00FF)) << 8;
					W[z >> 2] = ((W[z >> 2] | (tar[2] & 0x00FF)) << 8)
							| (tar[3] & 0x00FF);
				}
			}
			p0 = 0;
			A = h0;
			B = h1;
			C = h2;
			D = h3;
			E = h4;

			r0(0, K0);
			r0(0, K0);
			r0(0, K0);
			r0(0, K0);
			r0(0, K0);
			r0(0, K0);
			r0(0, K0);
			r0(0, K0);
			r0(0, K0);
			r0(0, K0);
			r0(0, K0);
			r0(0, K0);
			r0(0, K0);
			r0(0, K0);
			r0(0, K0);
			r0(0, K0);

			p1 = 13;
			p2 = 8;
			p3 = 2;
			p4 = 0;
			r1(0, K0);
			r1(0, K0);
			r1(0, K0);
			r1(0, K0);

			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);
			r1(1, K1);

			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);
			r1(2, K2);

			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);
			r1(3, K3);

			h0 += A;
			h1 += B;
			h2 += C;
			h3 += D;
			h4 += E;

			// System.gc();
			if (nread <= 56)
				break;
		}

		W = null;
		BW = null;
		s = null;
		A = 0;
		B = 0;
		C = 0;
		D = 0;
		E = 0;
		temp = 0;
		h4 = 0;
		h3 = 0;
		h2 = 0;
		h1 = 0;
		p0 = 0;
		p1 = 0;
		p2 = 0;
		p3 = 0;
		p4 = 0;

		int highBit, lowBit;

		lowBit = (int) (h0 & 0x0FFFF);
		highBit = (int) ((h0 >> 16) & 0x0FFFFF);
		h0 = 0;
		String shaHigh = new String(Integer.toHexString(highBit));

		int leng = shaHigh.length();
		if (leng > 4) {
			shaHigh = shaHigh.substring(leng - 4);

		} else if (leng < 4) {
			for (int m = 0; m < 4 - leng; m++)
				shaHigh = "0" + shaHigh;
		}

		String shaLow = new String(Integer.toHexString(lowBit));

		leng = shaLow.length();
		if (leng > 4) {
			shaLow = shaLow.substring(leng - 4);

		} else if (leng < 4) {
			for (int m = 0; m < 4 - leng; m++)
				shaLow = "0" + shaLow;
		}
		return Long.parseLong(shaHigh + shaLow, 16);
	}

}
