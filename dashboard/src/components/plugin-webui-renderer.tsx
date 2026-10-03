import { useId, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Bar, BarChart, CartesianGrid, Line, LineChart, XAxis, YAxis } from 'recharts'

import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { ChartContainer, ChartTooltip, ChartTooltipContent } from '@/components/ui/chart'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { resolveNodeValue } from '@/lib/plugin-webui'
import type { Scalar, WebUINode } from '@/lib/plugin-webui'

interface RendererProps {
  nodes: WebUINode[]
  data: Record<string, unknown>
  values: Record<string, Scalar>
  busy: boolean
  pendingData?: boolean
  onChange: (name: string, value: Scalar) => void
  onAction: (name: string) => void
}

function display(value: unknown): string {
  if (value === null || value === undefined) return '—'
  if (typeof value === 'object') throw new Error('Expected a scalar display value')
  return String(value)
}

function NodeRenderer({ node, ...props }: Omit<RendererProps, 'nodes'> & { node: WebUINode }) {
  const { t } = useTranslation()
  const id = useId()
  const [tablePage, setTablePage] = useState(0)
  const awaitingData = props.pendingData && node.value !== null && typeof node.value === 'object'
  const value = awaitingData ? null : resolveNodeValue(node, props.data)
  const children = <PluginWebUIRenderer {...props} nodes={node.children} />
  const fieldValue = node.name === null ? null : props.values[node.name]
  const change = (next: Scalar) => {
    if (node.name !== null) props.onChange(node.name, next)
  }
  const heading = node.label ? <Label htmlFor={id}>{node.label}</Label> : null

  if (awaitingData)
    return (
      <p role="status" className="text-muted-foreground text-sm">
        {props.busy ? t('pluginWebUI.loading') : t('pluginWebUI.empty')}
      </p>
    )

  switch (node.type) {
    case 'stack':
      return (
        <section className="space-y-4">
          {heading}
          {children}
        </section>
      )
    case 'grid': {
      const columns = {
        1: 'md:grid-cols-1',
        2: 'md:grid-cols-2',
        3: 'md:grid-cols-3',
        4: 'md:grid-cols-4',
      }
      return (
        <section>
          {heading}
          <div
            className={`grid grid-cols-1 gap-4 ${columns[node.columns as keyof typeof columns]}`}
          >
            {children}
          </div>
        </section>
      )
    }
    case 'card':
      return (
        <Card>
          {node.label && (
            <CardHeader>
              <CardTitle>{node.label}</CardTitle>
            </CardHeader>
          )}
          <CardContent className="space-y-4 pt-6">{children}</CardContent>
        </Card>
      )
    case 'tabs':
      return (
        <Tabs defaultValue="0">
          <TabsList className="max-w-full overflow-x-auto">
            {node.children.map((child, index) => (
              <TabsTrigger key={index} value={String(index)}>
                {child.label}
              </TabsTrigger>
            ))}
          </TabsList>
          {node.children.map((child, index) => (
            <TabsContent key={index} value={String(index)}>
              <NodeRenderer {...props} node={child} />
            </TabsContent>
          ))}
        </Tabs>
      )
    case 'text':
      return (
        <div className="space-y-2">
          {heading}
          <p className="text-muted-foreground break-words whitespace-pre-wrap">{display(value)}</p>
        </div>
      )
    case 'stat':
      return (
        <Card>
          <CardHeader>
            <CardTitle className="text-muted-foreground text-sm">{node.label}</CardTitle>
          </CardHeader>
          <CardContent className="text-3xl font-semibold">{display(value)}</CardContent>
        </Card>
      )
    case 'input':
    case 'date':
      return (
        <div className="space-y-2">
          {heading}
          <Input
            id={id}
            disabled={props.busy}
            type={
              node.type === 'date' ? 'date' : typeof node.value === 'number' ? 'number' : 'text'
            }
            value={fieldValue === null || fieldValue === undefined ? '' : String(fieldValue)}
            maxLength={4000}
            onChange={(event) =>
              change(
                typeof node.value === 'number' && event.target.value !== ''
                  ? Number(event.target.value)
                  : event.target.value
              )
            }
          />
        </div>
      )
    case 'switch':
      return (
        <div className="flex items-center gap-3">
          <Switch
            id={id}
            disabled={props.busy}
            checked={fieldValue === true}
            onCheckedChange={change}
          />
          {heading}
        </div>
      )
    case 'select':
      return (
        <div className="space-y-2">
          {heading}
          <Select
            disabled={props.busy}
            value={typeof fieldValue === 'string' ? fieldValue : undefined}
            onValueChange={change}
          >
            <SelectTrigger id={id}>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {node.options.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )
    case 'button':
      return (
        <Button
          disabled={props.busy}
          variant={
            node.variant === 'danger'
              ? 'destructive'
              : node.variant === 'muted'
                ? 'secondary'
                : 'default'
          }
          onClick={() => {
            if (node.action !== null) props.onAction(node.action)
          }}
        >
          {node.label}
        </Button>
      )
    case 'table': {
      if (
        !Array.isArray(value) ||
        value.some((row) => row === null || typeof row !== 'object' || Array.isArray(row))
      )
        throw new Error('Table data must be an array of objects')
      const rows = value as Record<string, unknown>[]
      const columns = node.columns as Array<{ field: string; label: string }>
      const lastPage = Math.max(0, Math.ceil(rows.length / 50) - 1)
      const current = Math.min(tablePage, lastPage)
      return (
        <div className="space-y-3">
          {heading}
          <Table>
            <TableHeader>
              <TableRow>
                {columns.map((column) => (
                  <TableHead key={column.field}>{column.label}</TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.slice(current * 50, (current + 1) * 50).map((row, index) => (
                <TableRow key={index}>
                  {columns.map((column) => (
                    <TableCell key={column.field}>{display(row[column.field])}</TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
          {rows.length === 0 && (
            <p className="text-muted-foreground text-sm">{t('pluginWebUI.empty')}</p>
          )}
          {lastPage > 0 && (
            <div className="flex items-center gap-3">
              <Button
                variant="outline"
                disabled={current === 0}
                onClick={() => setTablePage(current - 1)}
              >
                {t('pluginWebUI.previous')}
              </Button>
              <span>
                {current + 1} / {lastPage + 1}
              </span>
              <Button
                variant="outline"
                disabled={current === lastPage}
                onClick={() => setTablePage(current + 1)}
              >
                {t('pluginWebUI.next')}
              </Button>
            </div>
          )}
        </div>
      )
    }
    case 'chart': {
      if (
        !Array.isArray(value) ||
        value.length > 2000 ||
        value.some(
          (row) =>
            row === null ||
            typeof row !== 'object' ||
            !node.y ||
            typeof row[node.y] !== 'number' ||
            !Number.isFinite(row[node.y]) ||
            !node.x ||
            !['string', 'number'].includes(typeof row[node.x])
        )
      )
        throw new Error(
          'Chart data must contain scalar x and numeric y fields, with at most 2000 rows'
        )
      const x = node.x as string
      const y = node.y as string
      const chartChildren = (
        <>
          <CartesianGrid vertical={false} />
          <XAxis dataKey={x} />
          <YAxis />
          <ChartTooltip content={<ChartTooltipContent />} />
        </>
      )
      return (
        <div className="space-y-3">
          {heading}
          <ChartContainer
            className="h-72 w-full"
            config={{ [y]: { label: node.label ?? y, color: 'var(--primary)' } }}
          >
            {node.chart_type === 'bar' ? (
              <BarChart data={value}>
                {chartChildren}
                <Bar dataKey={y} fill="var(--primary)" isAnimationActive={false} />
              </BarChart>
            ) : (
              <LineChart data={value}>
                {chartChildren}
                <Line dataKey={y} stroke="var(--primary)" dot={false} isAnimationActive={false} />
              </LineChart>
            )}
          </ChartContainer>
        </div>
      )
    }
  }
}

export function PluginWebUIRenderer({ nodes, ...props }: RendererProps) {
  return (
    <>
      {nodes.map((node, index) => (
        <NodeRenderer key={index} node={node} {...props} />
      ))}
    </>
  )
}
