import { Icon, addCollection } from '@iconify/react'
import streamlineSharpIcons from '@iconify-json/streamline-sharp/icons.json'
import { createElement } from 'react'

import { useTheme } from '@/components/use-theme'

import type { MenuIcon } from '@/components/layout/types'

addCollection(streamlineSharpIcons)

export function createStreamlineIcon(name: string, fallback?: MenuIcon): MenuIcon {
  return function StreamlineGeneratedIcon({ className, color, size = 20 }) {
    const { themeConfig } = useTheme()

    if (themeConfig.dashboardStyle !== 'future-retro' && fallback) {
      return createElement(fallback, { className, color, size })
    }

    return createElement(Icon, {
      icon: `streamline-sharp:${name}`,
      className,
      color,
      width: size,
      height: size,
    })
  }
}
