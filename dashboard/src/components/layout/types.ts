import type { ComponentType, ReactNode } from 'react'
import type { LucideProps } from 'lucide-react'

export interface LayoutProps {
  children: ReactNode
}

export type WorkspaceMode = 'settings' | 'chat' | 'logs'

export interface MenuItem {
  icon: ComponentType<LucideProps>
  label: string
  path: string
  searchDescription?: string
  tourId?: string
  featureFlag?: 'behaviorLearning'
}

export interface MenuSection {
  title: string
  items: MenuItem[]
}
