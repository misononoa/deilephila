export function truncateHex(hex: string, length = 8): string {
  return hex.slice(0, length) + "…";
}
