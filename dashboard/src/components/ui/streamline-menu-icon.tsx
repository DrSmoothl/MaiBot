import { Icon } from '@iconify/react'
import { createElement } from 'react'

import { useTheme } from '@/components/use-theme'
import { getMillenniumIcon } from './millennium-icon-set'
import { MillenniumIcon } from './millennium-icons'
import { getStreamlineIcon } from './streamline-icons'

import type { MenuIcon } from '@/components/layout/types'

export function createStreamlineIcon(name: string, fallback?: MenuIcon): MenuIcon {
  return function StreamlineGeneratedIcon({ className, color, size = 20 }) {
    const { themeConfig } = useTheme()
    const icon = getStreamlineIcon('streamline-sharp', name)

    // 千禧风格有自己的一套线条图标；没登记的名称退回到备用图标。
    if (themeConfig.dashboardStyle === 'millennium') {
      const millenniumIcon = getMillenniumIcon(name)
      if (millenniumIcon) {
        return createElement(MillenniumIcon, { icon: millenniumIcon, className, color, size })
      }
      if (fallback) return createElement(fallback, { className, color, size })
    }

    if ((themeConfig.dashboardStyle !== 'future-retro' || !icon) && fallback) {
      return createElement(fallback, { className, color, size })
    }

    return createElement(Icon, {
      icon: icon ?? `streamline-sharp:${name}`,
      className,
      color,
      width: size,
      height: size,
    })
  }
}
