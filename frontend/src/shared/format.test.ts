import { describe, expect, it } from 'vitest'
import { formatCount, formatPercent } from './format'

describe('analytics formatting', () => {
  it('formats counts and ratios without converting null to zero', () => {
    expect(formatCount(4000)).toBe('4.000')
    expect(formatPercent(0.49325)).toBe('49,3%')
    expect(formatCount(null)).toBe('Không có dữ liệu')
    expect(formatPercent(null)).toBe('Không có dữ liệu')
  })
})
