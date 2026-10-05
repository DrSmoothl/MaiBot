/**
 * 千禧风格的线条图标：24 网格、方头直角、统一 2.5 线宽，和键帽、像素字的硬朗感一致。
 * 以未来复古图标集的名称为键，没有登记的名称由调用方退回到备用图标。
 */

const MILLENNIUM_ICON_PATHS: Record<string, string> = {
  // 侧栏导航
  'allergens-fish-remix': 'M3 11l9-8 9 8M6 10v10h12V10M10 20v-5h4v5',
  'desktop-chat-remix': 'M3 4h18v12H3zM9 20h6M12 16v4M7 8h6M7 12h10',
  'page-setting-remix': 'M4 7h10M18 7h2M4 17h4M12 17h8M14 4v6M8 14v6',
  'module-remix': 'M12 3l8 4v10l-8 4-8-4V7zM4 7l8 4 8-4M12 11v10',
  'script-1-remix': 'M6 3h9l4 4v14H6zM15 3v4h4M9 12h7M9 16h7',
  'happy-face-remix': 'M4 4h16v16H4zM9 9v2M15 9v2M8 15h8',
  'chat-bubble-square-write-remix': 'M4 4h11M4 4v16h16V9M9 15l10-10 2 2-10 10H9z',
  'sign-hashtag-solid': 'M9 3L7 21M17 3l-2 18M4 9h17M3 15h17',
  'cyborg-solid':
    'M7 7h10v10H7zM10 10h4v4h-4zM9 3v4M15 3v4M9 17v4M15 17v4M3 9h4M3 15h4M17 9h4M17 15h4',
  'user-sticker-square-remix': 'M4 4h16v6H4zM4 10h16v10H4zM8 7h1M8 14h8M8 17h5',
  'application-add-remix': 'M4 4h7v7H4zM4 13h7v7H4zM13 13h7v7h-7M16.5 4v7M13 7.5h7',
  'router-wifi-network-solid': 'M3 14h18v6H3zM7 17h1M11 17h1M17 14V8M13 7l4-4 4 4',
  'store-2-solid': 'M3 9l2-5h14l2 5v3H3zM5 12v8h14v-8M10 20v-5h4v5',
  // 搜索与通用
  'file-bookmark-solid': 'M6 3h9l4 4v14H6zM15 3v4h4M9 11h4v6l-2-2-2 2z',
  'horizontal-slider-2-solid': 'M4 7h10M18 7h2M4 17h4M12 17h8M14 4v6M8 14v6',
  'search-bar-solid': 'M4 4h11v11H4zM15 15l6 6',
  'edit-pdf-solid': 'M4 20h4L20 8l-4-4L4 16z',
  'delete-2-solid': 'M4 7h16M9 7V4h6v3M6 7v13h12V7M10 11v6M14 11v6',
  'line-arrow-right-1-remix': 'M9 5l7 7-7 7',
  'information-circle-solid': 'M4 4h16v16H4zM12 11v6M12 7v1',
}

// eslint-disable-next-line react-refresh/only-export-components
export function getMillenniumIconPath(name: string): string | undefined {
  return MILLENNIUM_ICON_PATHS[name]
}

interface MillenniumIconProps {
  path: string
  className?: string
  color?: string
  size?: number | string
}

export function MillenniumIcon({ path, className, color, size = 20 }: MillenniumIconProps) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 24 24"
      width={size}
      height={size}
      fill="none"
      stroke={color ?? 'currentColor'}
      strokeWidth={2.5}
      strokeLinecap="square"
      strokeLinejoin="miter"
      aria-hidden="true"
      data-millennium-icon="true"
      className={className}
    >
      <path d={path} />
    </svg>
  )
}
