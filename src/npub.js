// Minimal bech32 (BIP-173) decoder for npub → hex pubkey. No dependencies.
// Only what the dashboard search needs: decode, verify checksum, convert.

const CHARSET = 'qpzry9x8gf2tvdw0s3jn54khce6mua7l';

function bech32Polymod(values) {
  const GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];
  let chk = 1;
  for (const v of values) {
    const b = chk >> 25;
    chk = ((chk & 0x1ffffff) << 5) ^ v;
    for (let i = 0; i < 5; i++) if ((b >> i) & 1) chk ^= GEN[i];
  }
  return chk;
}

function hrpExpand(hrp) {
  const out = [];
  for (const c of hrp) out.push(c.charCodeAt(0) >> 5);
  out.push(0);
  for (const c of hrp) out.push(c.charCodeAt(0) & 31);
  return out;
}

function convertBits(data, fromBits, toBits, pad) {
  let acc = 0, bits = 0;
  const ret = [];
  const maxv = (1 << toBits) - 1;
  for (const value of data) {
    if (value < 0 || value >> fromBits) return null;
    acc = (acc << fromBits) | value;
    bits += fromBits;
    while (bits >= toBits) {
      bits -= toBits;
      ret.push((acc >> bits) & maxv);
    }
  }
  if (pad) {
    if (bits) ret.push((acc << (toBits - bits)) & maxv);
  } else if (bits >= fromBits || ((acc << (toBits - bits)) & maxv)) {
    return null;
  }
  return ret;
}

function npubToHex(npub) {
  if (typeof npub !== 'string' || npub.length < 8) return null;
  const pos = npub.lastIndexOf('1');
  if (pos < 1 || pos + 7 > npub.length) return null;
  const hrp = npub.slice(0, pos).toLowerCase();
  if (hrp !== 'npub') return null;
  const data = [];
  for (const c of npub.slice(pos + 1)) {
    const v = CHARSET.indexOf(c.toLowerCase());
    if (v === -1) return null;
    data.push(v);
  }
  if (bech32Polymod(hrpExpand(hrp).concat(data)) !== 1) return null;
  const bytes = convertBits(data.slice(0, -6), 5, 8, false);
  if (!bytes || bytes.length !== 32) return null;
  return bytes.map(b => b.toString(16).padStart(2, '0')).join('');
}

module.exports = { npubToHex };
