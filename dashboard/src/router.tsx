import {
  createRootRoute,
  createRoute,
  createRouter,
  lazyRouteComponent,
  Outlet,
  redirect,
} from '@tanstack/react-router'
import { TanStackRouterDevtools } from '@tanstack/router-devtools'
import { NotFoundPage } from './routes/404'
import { Layout } from './components/layout'
import { RoutePendingFallback } from './components/route-pending-fallback'
import { checkAuth } from './hooks/use-auth'
import { RouteErrorBoundary } from './components/error-boundary'

// Root 路由
const rootRoute = createRootRoute({
  component: () => (
    <>
      <Outlet />
      {import.meta.env.DEV && <TanStackRouterDevtools />}
    </>
  ),
  beforeLoad: () => {
    // 如果访问根路径且未认证，重定向到认证页面
    if (window.location.pathname === '/' && !checkAuth()) {
      throw redirect({ to: '/auth' })
    }
  },
})

// 认证路由（无 Layout）
const authRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/auth',
  component: lazyRouteComponent(() => import('./routes/auth'), 'AuthPage'),
})

// 首次配置路由（无 Layout）
const setupRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/setup',
  component: lazyRouteComponent(() => import('./routes/setup/index.tsx'), 'SetupPage'),
})

// 受保护的路由 Root（带 Layout）
const protectedRoute = createRoute({
  getParentRoute: () => rootRoute,
  id: 'protected',
  component: () => (
    <Layout>
      <Outlet />
    </Layout>
  ),
  errorComponent: ({ error }) => <RouteErrorBoundary error={error} />,
})

// 首页路由
const indexRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/',
  component: lazyRouteComponent(() => import('./routes/index'), 'IndexPage'),
})

// 配置路由 - 麦麦主程序配置
const botConfigRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/config/bot',
  component: lazyRouteComponent(() => import('./routes/config/bot'), 'BotConfigPage'),
})

// 配置路由 - 旧模型厂商配置入口，已合并到模型配置页
const modelProviderConfigRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/config/modelProvider',
  beforeLoad: () => {
    throw redirect({ to: '/config/model' })
  },
})

// 配置路由 - 麦麦模型配置
const modelConfigRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/config/model',
  component: lazyRouteComponent(() => import('./routes/config/model'), 'ModelConfigPage'),
})

// 配置路由 - 麦麦适配器配置（已停用，引导跳转到插件配置；旧实现保留在 ./routes/config/adapter）
const promptManagementRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/config/prompts',
  component: lazyRouteComponent(() => import('./routes/config/prompts'), 'PromptManagementPage'),
})

// 配置路由 - 人设生成器（测试功能）
const promptGeneratorRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/config/prompt-generator',
  component: lazyRouteComponent(() => import('./routes/prompt-generator'), 'PromptGeneratorPage'),
})

const adapterConfigRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/config/adapter',
  component: lazyRouteComponent(() => import('./routes/config/adapter-disabled'), 'AdapterConfigPage'),
})

// 资源管理路由 - 表情包管理
const emojiManagementRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/resource/emoji',
  component: lazyRouteComponent(
    () => import('./routes/resource/emoji/index.tsx'),
    'EmojiManagementPage'
  ),
})

// 资源管理路由 - 表达方式管理
const expressionManagementRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/resource/expression',
  component: lazyRouteComponent(
    () => import('./routes/resource/expression/index.tsx'),
    'ExpressionManagementPage'
  ),
})

// 资源管理路由 - 人物信息管理
const personManagementRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/resource/person',
  component: lazyRouteComponent(() => import('./routes/person'), 'PersonManagementPage'),
})

// 资源管理路由 - 黑话管理
const jargonManagementRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/resource/jargon',
  component: lazyRouteComponent(
    () => import('./routes/resource/jargon/index.tsx'),
    'JargonManagementPage'
  ),
})

// 资源管理路由 - 知识库图谱可视化
const behaviorLearningRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/resource/behavior',
  component: lazyRouteComponent(
    () => import('./routes/resource/behavior/index.tsx'),
    'BehaviorLearningPage'
  ),
})

const knowledgeGraphRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/resource/knowledge-graph',
  component: lazyRouteComponent(
    () => import('./routes/resource/knowledge-graph/index.tsx'),
    'KnowledgeGraphPage'
  ),
})

// 资源管理路由 - 麦麦知识库管理
const knowledgeBaseRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/resource/knowledge-base',
  component: lazyRouteComponent(
    () => import('./routes/resource/knowledge-base'),
    'KnowledgeBasePage'
  ),
})

// 日志查看器路由
const logsRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/logs',
  component: lazyRouteComponent(() => import('./routes/logs'), 'LogViewerPage'),
})

const reasoningProcessRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/reasoning-process',
  component: lazyRouteComponent(() => import('./routes/logs'), 'ReasoningLogViewerPage'),
})

// MaiSaka 聊天流监控路由
const plannerMonitorRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/planner-monitor',
  component: lazyRouteComponent(() => import('./routes/monitor/index.tsx'), 'PlannerMonitorPage'),
})

// 本地聊天室路由
const chatRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/chat',
  component: lazyRouteComponent(() => import('./routes/chat/index'), 'ChatPage'),
})

// 外部程序嵌入用聊天室路由，不挂载 dashboard 顶栏
const chatEmbedRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/chat/embed',
  component: lazyRouteComponent(() => import('./routes/chat/embed'), 'ChatEmbedPage'),
})

// 外部程序嵌入用插件市场路由，不挂载 dashboard 顶栏和侧边栏
const pluginsEmbedRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/plugins/embed',
  component: lazyRouteComponent(
    () => import('./routes/plugins/embed'),
    'PluginMarketplaceEmbedPage'
  ),
})

// 插件市场路由
const pluginsRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/plugins',
  component: lazyRouteComponent(
    () => import('./routes/plugins/PluginMarketplacePage'),
    'PluginMarketplacePage'
  ),
})

// 插件详情路由
const pluginDetailRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/plugin-detail',
  component: lazyRouteComponent(() => import('./routes/plugin-detail'), 'PluginDetailPage'),
})

// 模型分配预设市场路由
const modelPresetsRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/model-presets',
  component: lazyRouteComponent(() => import('./routes/model-presets'), 'ModelPresetsPage'),
})

// 插件配置路由
const pluginConfigRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/plugin-config',
  component: lazyRouteComponent(() => import('./routes/plugin-config'), 'PluginConfigPage'),
})

// 外部程序嵌入用插件配置路由，不挂载 dashboard 顶栏和侧边栏
const pluginConfigEmbedRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/plugin-config/embed',
  component: lazyRouteComponent(
    () => import('./routes/plugin-config-embed'),
    'PluginConfigEmbedPage'
  ),
})

// 插件镜像源配置路由
const pluginMirrorsRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/plugin-mirrors',
  component: lazyRouteComponent(() => import('./routes/plugin-mirrors'), 'PluginMirrorsPage'),
})

// 设置页路由
const mcpSettingsRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/mcp-settings',
  component: lazyRouteComponent(() => import('./routes/mcp-settings'), 'MCPSettingsPage'),
})

const settingsRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/settings',
  component: lazyRouteComponent(() => import('./routes/settings/index.tsx'), 'SettingsPage'),
})

// 配置模板市场路由
const packMarketRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/config/pack-market',
  component: lazyRouteComponent(() => import('./routes/config/pack-market')),
})

// 配置模板详情路由
export const packDetailRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/config/pack-market/$packId',
  component: lazyRouteComponent(() => import('./routes/config/pack-detail')),
})

// 问卷调查路由 - WebUI 反馈
const webuiFeedbackSurveyRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/survey/webui-feedback',
  component: lazyRouteComponent(
    () => import('./routes/survey/webui-feedback'),
    'WebUIFeedbackSurveyPage'
  ),
})

// 问卷调查路由 - 麦麦体验反馈
const maibotFeedbackSurveyRoute = createRoute({
  getParentRoute: () => protectedRoute,
  path: '/survey/maibot-feedback',
  component: lazyRouteComponent(
    () => import('./routes/survey/maibot-feedback'),
    'MaiBotFeedbackSurveyPage'
  ),
})

// 404 路由
const notFoundRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '*',
  component: NotFoundPage,
})

// 路由树
const routeTree = rootRoute.addChildren([
  authRoute,
  setupRoute,
  chatEmbedRoute,
  pluginsEmbedRoute,
  pluginConfigEmbedRoute,
  protectedRoute.addChildren([
    indexRoute,
    botConfigRoute,
    modelProviderConfigRoute,
    modelConfigRoute,
    promptManagementRoute,
    promptGeneratorRoute,
    adapterConfigRoute,
    emojiManagementRoute,
    expressionManagementRoute,
    jargonManagementRoute,
    behaviorLearningRoute,
    personManagementRoute,
    knowledgeGraphRoute,
    knowledgeBaseRoute,
    pluginsRoute,
    pluginDetailRoute,
    modelPresetsRoute,
    pluginConfigRoute,
    pluginMirrorsRoute,
    mcpSettingsRoute,
    logsRoute,
    reasoningProcessRoute,
    plannerMonitorRoute,
    chatRoute,
    settingsRoute,
    packMarketRoute,
    packDetailRoute,
    webuiFeedbackSurveyRoute,
    maibotFeedbackSurveyRoute,
  ]),
  notFoundRoute,
])

type RouteNode = {
  fullPath?: string
  children?: RouteNode[]
}

function collectRoutePaths(node: RouteNode): string[] {
  const currentPath = node.fullPath ? [node.fullPath] : []
  const childPaths = node.children?.flatMap(collectRoutePaths) ?? []
  return [...currentPath, ...childPaths]
}

export const registeredRoutePaths = new Set(collectRoutePaths(routeTree as RouteNode))

// 创建路由器
export const router = createRouter({
  routeTree,
  defaultNotFoundComponent: NotFoundPage,
  defaultErrorComponent: ({ error }) => <RouteErrorBoundary error={error} />,
  defaultPendingComponent: RoutePendingFallback,
  defaultPendingMs: 120,
  defaultPendingMinMs: 120,
  defaultPreload: 'intent',
  defaultPreloadDelay: 80,
})

// 类型声明
declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router
  }
}
