import { ExternalLink, Newspaper, Pin, RefreshCw } from 'lucide-react'
import { useState } from 'react'
import { useTranslation } from 'react-i18next'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Dialog,
  DialogBody,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { MarkdownRenderer } from '@/components/markdown-renderer'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Skeleton } from '@/components/ui/skeleton'
import type { NewsItem } from '@/lib/news-api'
import { cn } from '@/lib/utils'

import { useNews } from './hooks/useNews'

const RELATIVE_TIME_DAY_THRESHOLD = 30

/** 资讯发布时间相对当前时间的展示（如「2小时前」），超过 30 天回退为日期。 */
function formatRelativeTime(value: string, locale: string): string {
  const publishedTime = new Date(value).getTime()
  if (Number.isNaN(publishedTime)) return ''

  const diffSeconds = Math.round((publishedTime - Date.now()) / 1000)
  const absoluteSeconds = Math.abs(diffSeconds)
  if (absoluteSeconds >= RELATIVE_TIME_DAY_THRESHOLD * 24 * 60 * 60) {
    return new Date(publishedTime).toLocaleDateString(locale)
  }

  const formatter = new Intl.RelativeTimeFormat(locale, { numeric: 'auto' })
  if (absoluteSeconds < 60) return formatter.format(diffSeconds, 'second')
  if (absoluteSeconds < 60 * 60) return formatter.format(Math.round(diffSeconds / 60), 'minute')
  if (absoluteSeconds < 24 * 60 * 60) return formatter.format(Math.round(diffSeconds / 3600), 'hour')
  return formatter.format(Math.round(diffSeconds / 86400), 'day')
}

function NewsRow({
  item,
  locale,
  openLabel,
  pinnedLabel,
  onOpen,
}: {
  item: NewsItem
  locale: string
  openLabel: string
  pinnedLabel: string
  onOpen: () => void
}) {
  const hasLink = item.url.trim().length > 0

  return (
    <div
      role="button"
      tabIndex={0}
      title={openLabel}
      className="hover:bg-accent/40 block w-full cursor-pointer rounded-md border p-3 text-left transition-colors focus-visible:ring-2 focus-visible:outline-none"
      onClick={onOpen}
      onKeyDown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault()
          onOpen()
        }
      }}
    >
      <div className="flex items-start gap-2">
        {item.pinned && (
          <Pin className="text-primary mt-0.5 h-3.5 w-3.5 shrink-0" aria-label={pinnedLabel} />
        )}
        <span className="min-w-0 flex-1 text-sm font-medium">{item.title}</span>
        {hasLink && (
          <a
            href={item.url}
            target="_blank"
            rel="noopener noreferrer"
            title={openLabel}
            aria-label={openLabel}
            className="text-muted-foreground hover:text-foreground shrink-0"
            onClick={(event) => event.stopPropagation()}
          >
            <ExternalLink className="h-3.5 w-3.5" />
          </a>
        )}
      </div>
      {item.summary && (
        <p className="text-muted-foreground mt-1 line-clamp-2 text-xs leading-relaxed">
          {item.summary}
        </p>
      )}
      <div className="text-muted-foreground mt-2 flex items-center gap-2 text-[11px]">
        {item.source && <span className="min-w-0 shrink-0 truncate">{item.source}</span>}
        <span className="ml-auto shrink-0 tabular-nums">
          {formatRelativeTime(item.published_at, locale)}
        </span>
      </div>
    </div>
  )
}

export function NewsCard() {
  const { t, i18n } = useTranslation()
  const { news, isNewsLoading, newsError, fetchNews } = useNews()
  const [selectedNews, setSelectedNews] = useState<NewsItem | null>(null)
  const locale = i18n.resolvedLanguage || i18n.language

  return (
    <>
      <Card className="flex h-full min-h-0 flex-col">
        <CardHeader className="flex flex-row items-center justify-between gap-2 pb-3">
          <CardTitle className="flex items-center gap-2 text-sm font-medium">
            <Newspaper className="h-4 w-4" />
            {t('home.news.title')}
          </CardTitle>
          <Button
            variant="ghost"
            size="icon"
            className="h-7 w-7"
            aria-label={t('home.news.refresh')}
            title={t('home.news.refresh')}
            disabled={isNewsLoading}
            onClick={() => void fetchNews(true)}
          >
            <RefreshCw className={cn('h-3.5 w-3.5', isNewsLoading && 'animate-spin')} />
          </Button>
        </CardHeader>
        <CardContent className="flex min-h-0 flex-1 flex-col">
          {isNewsLoading && !news ? (
            <div className="flex min-h-0 flex-1 flex-col justify-center space-y-2">
              {[0, 1].map((index) => (
                <div key={index} className="rounded-md border p-2.5">
                  <Skeleton className="h-4 w-3/4" />
                  <Skeleton className="mt-1.5 h-3 w-full" />
                </div>
              ))}
            </div>
          ) : newsError && !news ? (
            <div className="flex min-h-0 flex-1 flex-col items-center justify-center gap-3 text-sm">
              <span className="text-destructive">{newsError}</span>
              <Button variant="outline" size="sm" onClick={() => void fetchNews(true)}>
                <RefreshCw className="mr-2 h-4 w-4" />
                {t('home.news.retry')}
              </Button>
            </div>
          ) : news && news.length > 0 ? (
            <ScrollArea className="h-full pr-3">
              <div className="space-y-2">
                {news.map((item, index) => (
                  <NewsRow
                    key={`${item.id || item.published_at}-${index}`}
                    item={item}
                    locale={locale}
                    openLabel={t('home.news.open')}
                    pinnedLabel={t('home.news.pinned')}
                    onOpen={() => setSelectedNews(item)}
                  />
                ))}
              </div>
            </ScrollArea>
          ) : (
            <div className="text-muted-foreground flex min-h-0 flex-1 items-center justify-center text-sm">
              {t('home.news.empty')}
            </div>
          )}
        </CardContent>
      </Card>

      <Dialog open={selectedNews !== null} onOpenChange={(open) => !open && setSelectedNews(null)}>
        <DialogContent className="[--dialog-width:48rem]">
          {selectedNews && (
            <>
              <DialogHeader>
                <DialogTitle className="pr-6">{selectedNews.title}</DialogTitle>
                <DialogDescription>
                  {[selectedNews.pinned ? t('home.news.pinned') : '', selectedNews.source, formatRelativeTime(selectedNews.published_at, locale)]
                    .filter(Boolean)
                    .join(' · ')}
                </DialogDescription>
              </DialogHeader>
              <DialogBody>
                <MarkdownRenderer content={selectedNews.content || selectedNews.summary} />
              </DialogBody>
              {selectedNews.url && (
                <div className="flex justify-end">
                  <Button asChild variant="outline" size="sm">
                    <a href={selectedNews.url} target="_blank" rel="noopener noreferrer">
                      <ExternalLink className="mr-2 h-4 w-4" />
                      {t('home.news.openLink')}
                    </a>
                  </Button>
                </div>
              )}
            </>
          )}
        </DialogContent>
      </Dialog>
    </>
  )
}
