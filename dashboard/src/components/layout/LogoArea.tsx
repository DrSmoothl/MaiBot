import { cn } from '@/lib/utils'
import { formatVersion } from '@/lib/version'

interface LogoAreaProps {
  sidebarOpen: boolean
}

export function LogoArea({ sidebarOpen }: LogoAreaProps) {
  return (
    <div
      data-dashboard-logo-area="true"
      className="flex h-[var(--layout-sidebar-logo-height)] items-center border-b px-[var(--layout-sidebar-logo-padding-x)]"
    >
      <div
        className={cn(
          'relative flex items-center justify-center flex-1 transition-all overflow-hidden',
          // 移动端始终完整显示,桌面端根据 sidebarOpen 切换
          'lg:flex-1',
          !sidebarOpen && 'lg:flex-none lg:w-8'
        )}
      >
        {/* 移动端始终显示完整 Logo，桌面端根据 sidebarOpen 切换 */}
        <div className={cn(
          "flex min-w-0 flex-col items-start justify-center gap-1",
          !sidebarOpen && "lg:hidden"
        )}>
          <span
            data-dashboard-logo-title="true"
            className="max-w-full truncate whitespace-nowrap text-xl font-bold text-primary-gradient"
          >
            MaiBot WebUI
          </span>
          <div className="flex max-w-full items-center gap-2 overflow-hidden">
            <span className="shrink-0 whitespace-nowrap text-sm font-semibold text-primary/70">
              {formatVersion()}
            </span>
          </div>
          <div className="hidden">
            <span className="font-bold text-xl text-primary-gradient whitespace-nowrap">MaiBot WebUI</span>
            <span className="text-base font-semibold text-primary/70 whitespace-nowrap">
              {formatVersion()}
            </span>
          </div>
        </div>
        {/* 折叠时的 Logo - 仅桌面端显示 */}
        {!sidebarOpen && (
          <span className="hidden lg:block font-bold text-primary-gradient text-2xl">M</span>
        )}
      </div>
    </div>
  )
}
