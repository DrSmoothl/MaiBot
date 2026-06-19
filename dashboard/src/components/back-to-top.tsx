import { useRouterState } from '@tanstack/react-router'
import { ArrowUp } from 'lucide-react'
import { type CSSProperties, useEffect, useRef, useState } from 'react'

import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'

export function BackToTop() {
  const [progress, setProgress] = useState(0)
  const [visible, setVisible] = useState(false)
  const scrollerRef = useRef<HTMLElement | null>(null)
  const locationKey = useRouterState({
    select: (state) => state.location.pathname,
  })

  useEffect(() => {
    const handleScroll = (e: Event) => {
      const target = e.target as HTMLElement
      
      // 简单的启发式：如果是主要滚动容器（通常高度较大）
      // 我们假设页面中主要的滚动区域是高度最大的那个，或者就是当前触发滚动的这个
      // 只要它有足够的滚动空间
      if (target.scrollHeight > target.clientHeight + 100) {
         scrollerRef.current = target
         
         const scrollTop = target.scrollTop
         const height = target.scrollHeight - target.clientHeight
         const scrolled = height > 0 ? (scrollTop / height) * 100 : 0
         
         setProgress(scrolled)
         setVisible(scrollTop > 300)
      }
    }

    const handleNavigation = () => {
      scrollerRef.current = null
      setProgress(0)
      setVisible(false)
    }

    // 使用捕获阶段监听所有滚动事件，因为 scroll 事件不冒泡
    window.addEventListener('scroll', handleScroll, { capture: true, passive: true })
    window.addEventListener('popstate', handleNavigation)
    window.addEventListener('hashchange', handleNavigation)
    return () => {
      window.removeEventListener('scroll', handleScroll, { capture: true })
      window.removeEventListener('popstate', handleNavigation)
      window.removeEventListener('hashchange', handleNavigation)
    }
  }, [])

  useEffect(() => {
    scrollerRef.current = null
    const frameId = window.requestAnimationFrame(() => {
      setProgress(0)
      setVisible(false)
    })

    return () => window.cancelAnimationFrame(frameId)
  }, [locationKey])

  const scrollToTop = () => {
    scrollerRef.current?.scrollTo({ top: 0, behavior: 'smooth' })
  }

  // SVG 环形进度条参数
  const radius = 18
  const circumference = 2 * Math.PI * radius
  const strokeDashoffset = circumference - (progress / 100) * circumference
  const squareProgress = Math.max(0, Math.min(101, progress >= 99.5 ? 101 : progress))
  const squareProgressStyle = {
    '--back-to-top-progress': `${squareProgress}%`,
  } as CSSProperties

  return (
    <div 
      className={cn(
        "fixed right-6 bottom-24 z-50 transform transition-all duration-500 ease-in-out",
        visible ? "translate-x-0 opacity-100" : "translate-x-32 opacity-0 pointer-events-none"
      )}
    >
      <Button
        variant="outline"
        size="icon"
        data-dashboard-back-to-top="true"
        className={cn(
          "relative h-10 w-10 rounded-full shadow-xl",
          "bg-background/80 backdrop-blur-md border-border/50",
          "hover:bg-accent hover:scale-105 hover:shadow-2xl hover:border-primary/50",
          "transition-all duration-300",
          "group"
        )}
        onClick={scrollToTop}
        aria-label="回到顶部"
      >
        {/* 进度环背景 */}
        <svg
          data-dashboard-back-to-top-progress="circle"
          className="absolute inset-0 h-full w-full -rotate-90 transform p-1"
          viewBox="0 0 44 44"
        >
          <circle
            className="text-muted-foreground/10"
            strokeWidth="3"
            stroke="currentColor"
            fill="transparent"
            r={radius}
            cx="22"
            cy="22"
          />
          {/* 进度环 */}
          <circle
            className="text-primary transition-all duration-100 ease-out"
            strokeWidth="3"
            strokeDasharray={circumference}
            strokeDashoffset={strokeDashoffset}
            strokeLinecap="round"
            stroke="currentColor"
            fill="transparent"
            r={radius}
            cx="22"
            cy="22"
          />
        </svg>

        <div
          data-dashboard-back-to-top-progress="square"
          className="pointer-events-none absolute inset-0 hidden"
          style={squareProgressStyle}
        />
        
        {/* 图标 */}
        <ArrowUp 
          className="h-4 w-4 text-primary transition-transform duration-300 group-hover:-translate-y-0.5 group-hover:scale-110" 
          strokeWidth={2.5}
        />
        
        {/* 内部发光效果 (仅在 dark 模式下明显) */}
        <div
          data-dashboard-back-to-top-glow="true"
          className="absolute inset-0 rounded-full bg-primary/5 opacity-0 group-hover:opacity-100 transition-opacity duration-300"
        />
      </Button>
    </div>
  )
}
