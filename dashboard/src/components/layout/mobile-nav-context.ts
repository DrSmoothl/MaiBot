import { createContext, useContext } from 'react'

import type { MenuSection } from './types'

export interface MobileNavContextValue {
  /** 与侧栏一致的菜单分组（含插件页面），移动端「管理」页据此生成入口 */
  menuSections: MenuSection[]
  /** 打开全局搜索 */
  openSearch: () => void
}

export const MobileNavContext = createContext<MobileNavContextValue>({
  menuSections: [],
  openSearch: () => {},
})

export function useMobileNav(): MobileNavContextValue {
  return useContext(MobileNavContext)
}

/** 「观察」标签覆盖的页面：麦麦观察、日志、推理过程与详细统计 */
const OBSERVE_PATH_PREFIXES = ['/planner-monitor', '/logs', '/reasoning-process', '/statistics']

export type MobileTab = 'home' | 'chat' | 'manage' | 'observe' | 'me'

/** 根据当前路径判断底部标签栏的选中项；不属于其它标签的页面都归到「管理」 */
export function resolveMobileTab(pathname: string): MobileTab {
  if (pathname === '/') return 'home'
  if (pathname === '/chat') return 'chat'
  if (pathname === '/settings') return 'me'
  if (OBSERVE_PATH_PREFIXES.some((prefix) => pathname.startsWith(prefix))) return 'observe'
  return 'manage'
}
