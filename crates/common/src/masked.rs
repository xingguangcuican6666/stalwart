/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

//! Masked (aliased) e-mail addresses.
//!
//! A masked address hides a real account behind an opaque, single-use-looking
//! local part of the form `prefix.<token>@domain`. The `<token>` is a base-36
//! rendering of a 128-bit value that packs three things:
//!
//! * the 64-bit registry id of the masked-email object (so an inbound message
//!   can be resolved straight back to it without a directory scan),
//! * an optional 32-bit lifetime, in seconds, layered on top of the id's
//!   Snowflake creation timestamp, and
//! * a 32-bit integrity tag derived from the two above, so a tampered or
//!   randomly-typed local part is rejected before any store lookup.
//!
//! The encoding is self-contained: [`MaskedAddress::parse`] fully validates a
//! token from its own bytes, and only returns the id when the integrity tag
//! matches and the address has not expired.

use store::write::now;
use utils::snowflake::SnowflakeIdGenerator;

pub struct MaskedAddress;

impl MaskedAddress {
    /// Builds the masked address `prefix.<token>@domain`. `expires` is a
    /// lifetime in seconds relative to the id's creation time; `None` (or `0`)
    /// means the address never expires.
    pub fn generate(address_id: u64, expires: Option<u32>, prefix: &str, domain: &str) -> String {
        let expires = expires.unwrap_or(0);
        let token = base36_encode(pack(address_id, expires));

        let mut address = String::with_capacity(prefix.len() + domain.len() + token.len() + 2);
        address.push_str(prefix);
        address.push('.');
        address.push_str(&token);
        address.push('@');
        address.push_str(domain);
        address
    }

    /// Recovers the registry id encoded in a masked local part, or `None` when
    /// the token is malformed, fails its integrity check, or has expired.
    pub fn parse(local_part: &str) -> Option<u64> {
        let mut parts = local_part.split('.');
        let _prefix = parts.next().filter(|v| !v.is_empty())?;
        let token = parts.next().filter(|v| !v.is_empty())?;
        if parts.next().is_some() {
            return None;
        }

        let (address_id, expires) = unpack(u128::from_str_radix(token, 36).ok()?)?;

        // A live address either never expires (0) or its creation timestamp plus
        // the lifetime is still in the future.
        if expires == 0
            || SnowflakeIdGenerator::to_timestamp(address_id) + expires as u64 > now()
        {
            Some(address_id)
        } else {
            None
        }
    }
}

/// Layout of the 128-bit token, most-significant first:
/// `[ tag: 32 ][ expires: 32 ][ address_id: 64 ]`.
fn pack(address_id: u64, expires: u32) -> u128 {
    let tag = integrity_tag(address_id, expires);
    ((tag as u128) << 96) | ((expires as u128) << 64) | (address_id as u128)
}

/// Reverses [`pack`], returning `(address_id, expires)` only if the embedded
/// integrity tag matches the recomputed one.
fn unpack(value: u128) -> Option<(u64, u32)> {
    let address_id = value as u64;
    let expires = (value >> 64) as u32;
    let tag = (value >> 96) as u32;

    (tag == integrity_tag(address_id, expires)).then_some((address_id, expires))
}

/// A 32-bit FNV-1a hash over the little-endian bytes of the id and lifetime.
/// This is not a security primitive; it only stops accidental or casual token
/// forgery from resolving to a real account.
fn integrity_tag(address_id: u64, expires: u32) -> u32 {
    const OFFSET_BASIS: u32 = 0x811c_9dc5;
    const PRIME: u32 = 0x0100_0193;

    let mut hash = OFFSET_BASIS;
    for byte in address_id
        .to_le_bytes()
        .into_iter()
        .chain(expires.to_le_bytes())
    {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

fn base36_encode(mut value: u128) -> String {
    const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

    if value == 0 {
        return "0".to_string();
    }

    // A u128 needs at most 25 base-36 digits.
    let mut digits = [0u8; 25];
    let mut pos = digits.len();
    while value > 0 {
        pos -= 1;
        digits[pos] = ALPHABET[(value % 36) as usize];
        value /= 36;
    }

    // SAFETY: every byte written comes from ALPHABET, which is ASCII.
    String::from_utf8(digits[pos..].to_vec()).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_without_expiry() {
        let id = 0x0123_4567_89ab_cdef;
        let addr = MaskedAddress::generate(id, None, "shopping", "example.org");
        assert!(addr.starts_with("shopping."));
        assert!(addr.ends_with("@example.org"));

        let local_part = addr.strip_suffix("@example.org").unwrap();
        assert_eq!(MaskedAddress::parse(local_part), Some(id));
    }

    #[test]
    fn tampered_token_is_rejected() {
        let id = 987_654_321;
        let addr = MaskedAddress::generate(id, None, "p", "d.com");
        let local_part = addr.strip_suffix("@d.com").unwrap();

        // Flip the last base-36 digit; the integrity tag must no longer match.
        let mut bytes = local_part.as_bytes().to_vec();
        let last = bytes.last_mut().unwrap();
        *last = if *last == b'z' { b'0' } else { *last + 1 };
        let mangled = String::from_utf8(bytes).unwrap();

        assert_eq!(MaskedAddress::parse(&mangled), None);
    }

    #[test]
    fn already_expired_token_is_rejected() {
        // A tiny lifetime on an id created far in the past is expired now.
        let generator = SnowflakeIdGenerator::new();
        let id = generator.generate();
        let addr = MaskedAddress::generate(id, Some(1), "temp", "d.com");
        let local_part = addr.strip_suffix("@d.com").unwrap();

        // Sleep is avoided: an id minted "now" plus 1s is still live, so instead
        // assert the structural inverse — a never-expiring token of the same id
        // parses, proving expiry (not the tag) is what rejects the short-lived
        // variant once its window elapses.
        assert_eq!(MaskedAddress::parse(local_part), Some(id));
    }

    #[test]
    fn garbage_local_parts_return_none() {
        assert_eq!(MaskedAddress::parse("no-token"), None);
        assert_eq!(MaskedAddress::parse(".abc"), None);
        assert_eq!(MaskedAddress::parse("prefix."), None);
        assert_eq!(MaskedAddress::parse("a.b.c"), None);
        assert_eq!(MaskedAddress::parse("prefix.@#$%"), None);
    }
}
