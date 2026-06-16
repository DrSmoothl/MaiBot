import { Icon, addCollection } from '@iconify/react'
import streamlineBlockIcons from '@iconify-json/streamline-block/icons.json'
import streamlineSharpIcons from '@iconify-json/streamline-sharp/icons.json'

import { useTheme } from '@/components/use-theme'
import { cn } from '@/lib/utils'

import type { ComponentType } from 'react'

addCollection(streamlineSharpIcons)
addCollection(streamlineBlockIcons)

type FallbackIcon = ComponentType<{
  className?: string
  color?: string
  size?: number | string
}>

interface StreamlineIconProps {
  name: string
  collection?: 'streamline-block' | 'streamline-sharp'
  fallback?: FallbackIcon
  className?: string
  color?: string
  size?: number | string
}

export function StreamlineIcon({
  name,
  collection = 'streamline-sharp',
  fallback: Fallback,
  className,
  color,
  size = 16,
}: StreamlineIconProps) {
  const { themeConfig } = useTheme()

  if (themeConfig.dashboardStyle !== 'future-retro' && Fallback) {
    return <Fallback className={className} color={color} size={size} />
  }

  return (
    <Icon
      icon={`${collection}:${name}`}
      className={cn('inline-block shrink-0', className)}
      color={color}
      width={size}
      height={size}
    />
  )
}
