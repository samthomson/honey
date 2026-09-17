// Cloudflare IP ranges — shared by the proxy (client-IP extraction) and the
// dashboard queries (collapsing CF WARP/Private Relay egress noise on maps).

const CF_V4_RANGES = [
  '173.245.48.0/20', '103.21.244.0/22', '103.22.200.0/22', '103.31.4.0/22',
  '141.101.64.0/18', '108.162.192.0/18', '190.93.240.0/20', '188.114.96.0/20',
  '197.234.240.0/22', '198.41.128.0/17', '162.158.0.0/15', '104.16.0.0/13',
  '104.24.0.0/14', '172.64.0.0/13', '131.0.72.0/22',
];

const CF_V6_RANGES = [
  '2400:cb00::/32', '2606:4700::/32', '2803:f800::/32',
  '2405:b500::/32', '2405:8100::/32', '2a06:98c0::/29',
  '2c0f:f248::/32',
];

function ipv4ToInt(ip) {
  const parts = ip.split('.').map(Number);
  if (parts.length !== 4 || parts.some(p => isNaN(p))) return null;
  return ((parts[0] << 24) + (parts[1] << 16) + (parts[2] << 8) + parts[3]) >>> 0;
}

function ipv6ToBigInt(ip) {
  // Handle :: shorthand expansion
  const halves = ip.split('::');
  let left = halves[0] ? halves[0].split(':') : [];
  let right = halves.length > 1 && halves[1] ? halves[1].split(':') : [];
  const missing = 8 - left.length - right.length;
  if (missing < 0) return null;
  const full = [...left, ...Array(missing).fill('0'), ...right];
  if (full.length !== 8) return null;
  const hex = full.map(g => g.padStart(4, '0')).join('');
  return BigInt('0x' + hex);
}

function isCloudflareIp(ip) {
  if (!ip) return false;
  const clean = ip.replace(/^::ffff:/, '');

  // IPv4
  if (clean.includes('.')) {
    const ipInt = ipv4ToInt(clean);
    if (ipInt === null) return false;
    return CF_V4_RANGES.some(cidr => {
      const [range, bits] = cidr.split('/');
      const rangeInt = ipv4ToInt(range);
      const mask = bits === '0' ? 0 : (0xFFFFFFFF << (32 - parseInt(bits))) >>> 0;
      return (ipInt & mask) === (rangeInt & mask);
    });
  }

  // IPv6
  if (clean.includes(':')) {
    const ipBig = ipv6ToBigInt(clean);
    if (ipBig === null) return false;
    return CF_V6_RANGES.some(cidr => {
      const [range, bits] = cidr.split('/');
      const rangeBig = ipv6ToBigInt(range);
      const bitCount = parseInt(bits);
      const mask = (1n << 128n) - (1n << BigInt(128 - bitCount));
      return (ipBig & mask) === (rangeBig & mask);
    });
  }

  return false;
}

module.exports = { isCloudflareIp, CF_V4_RANGES, CF_V6_RANGES };
