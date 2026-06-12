import { useTranslation } from 'react-i18next'

import { ScrollArea } from '@/components/ui/scroll-area'
import { cn } from '@/lib/utils'
import { useBackground } from '@/hooks/use-background'
import { BackgroundLayer } from '@/components/background-layer'

import { LogoArea } from './LogoArea'
import { NavItem } from './NavItem'
import { useMenuSections } from './use-menu-sections'

interface SidebarProps {
  sidebarOpen: boolean
  mobileMenuOpen: boolean
  tooltipsEnabled: boolean
  onMobileMenuClose: () => void
}

export function Sidebar({
  sidebarOpen,
  mobileMenuOpen,
  tooltipsEnabled,
  onMobileMenuClose,
}: SidebarProps) {
  const { t } = useTranslation()
  const { config: sidebarBg, inheritedFrom } = useBackground('sidebar')
  const inheritsPageBackground = inheritedFrom === 'page'
  const menuSections = useMenuSections()

  return (
    <aside
      data-dashboard-sidebar="true"
      className={cn(
        'fixed inset-y-0 left-0 isolate z-50 flex flex-col border-r transition-all duration-300 lg:relative lg:z-0 lg:h-full',
        inheritsPageBackground ? 'bg-transparent' : 'bg-card',
        // 移动端始终显示完整宽度，桌面端根据 sidebarOpen 切换
        'w-52 lg:w-auto',
        sidebarOpen ? 'lg:w-52' : 'lg:w-16',
        mobileMenuOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'
      )}
    >
      {!inheritsPageBackground && <BackgroundLayer config={sidebarBg} layerId="sidebar" />}

      {/* Logo 区域 */}
      <div className="relative z-10">
        <LogoArea sidebarOpen={sidebarOpen} />
      </div>

      <ScrollArea
        className={cn(
          'relative z-10',
          'min-h-0 flex-1 overflow-x-hidden',
          !sidebarOpen && 'lg:w-16'
        )}
        viewportClassName="[&>div]:!block"
      >
        <nav
          aria-label={t('a11y.sidebarNav')}
          className={cn('p-4', !sidebarOpen && 'lg:w-16 lg:p-2')}
        >
          <ul
            className={cn(
              // 移动端始终使用正常间距,桌面端根据 sidebarOpen 切换
              'space-y-4',
              !sidebarOpen && 'lg:w-full lg:space-y-3'
            )}
          >
            {menuSections.map((section, sectionIndex) => (
              <li key={section.title}>
                {/* 块标题 - 移动端始终可见，桌面端根据 sidebarOpen 切换 */}
                <div
                  className={cn(
                    'h-[1.25rem] px-3',
                    section.title === 'sidebar.groups.overview' && 'hidden',
                    // 移动端始终显示，桌面端根据状态切换
                    'mb-2',
                    !sidebarOpen && 'lg:invisible lg:mb-1'
                  )}
                >
                  <h3 className="text-muted-foreground/60 text-sm font-semibold tracking-wider whitespace-nowrap uppercase">
                    {t(section.title)}
                  </h3>
                </div>

                {/* 分割线 - 仅在桌面端折叠时显示 */}
                {!sidebarOpen && sectionIndex > 0 && (
                  <div className="border-border mb-2 hidden border-t lg:block" />
                )}

                {/* 菜单项列表 */}
                <ul className="space-y-1">
                  {section.items.map((item) => (
                    <NavItem
                      key={item.path}
                      item={item}
                      sidebarOpen={sidebarOpen}
                      tooltipsEnabled={tooltipsEnabled}
                      onMobileMenuClose={onMobileMenuClose}
                    />
                  ))}
                </ul>
              </li>
            ))}
          </ul>
        </nav>
      </ScrollArea>
    </aside>
  )
}
