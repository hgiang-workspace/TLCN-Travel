export const formatCount = (value: number | null) =>
  value === null ? 'Không có dữ liệu' : value.toLocaleString('vi-VN')

export const formatPercent = (value: number | null) =>
  value === null
    ? 'Không có dữ liệu'
    : new Intl.NumberFormat('vi-VN', {
        style: 'percent',
        maximumFractionDigits: 1,
      }).format(value)
