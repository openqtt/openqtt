//! Deterministic randomness, so that every run of the spike sees the same filters, topics and
//! placement. Nothing here needs to be cryptographic.

/// SplitMix64's finaliser: a stateless 64-bit mix, used to derive values from an index.
pub fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Mixes an index with a salt, so that independent properties of one item are uncorrelated.
pub fn hash2(a: u64, salt: u64) -> u64 {
    mix(a ^ mix(salt))
}

/// A uniform value in [0, 1) from 64 random bits.
pub fn unit(u: u64) -> f64 {
    // 53 bits fit an f64 mantissa exactly.
    (u >> 11) as f64 / (1u64 << 53) as f64
}

/// A sequential generator for streams of samples.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mix(self.0)
    }

    /// Uniform in [0, n). The modulo bias is irrelevant at the sizes used here.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

/// A Zipf distribution over ranks 0..n with exponent `s`, sampled by inverting its CDF.
pub struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    pub fn new(n: usize, s: f64) -> Self {
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0;
        for k in 1..=n {
            let w = 1.0 / (k as f64).powf(s);
            acc += w;
            cdf.push(acc);
        }
        for c in &mut cdf {
            *c /= acc;
        }
        Zipf { cdf }
    }

    /// The rank for a uniform value `u` in [0, 1).
    pub fn rank(&self, u: f64) -> usize {
        self.cdf
            .partition_point(|&c| c <= u)
            .min(self.cdf.len() - 1)
    }
}
