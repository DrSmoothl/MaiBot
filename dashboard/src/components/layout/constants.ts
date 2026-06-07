import { Activity, Boxes, BrainCircuit, Database, FileText, Hash, Home, MessageSquare, Network, Package, ScrollText, Sliders, Smile, Sparkles } from 'lucide-react'

import type { MenuSection } from './types'

export const menuSections: MenuSection[] = [
  {
    title: 'sidebar.groups.overview',
    items: [
      { icon: Home, label: 'sidebar.menu.home', path: '/', searchDescription: 'search.items.homeDesc' },
      { icon: Activity, label: 'sidebar.menu.maisakaMonitor', path: '/planner-monitor' },
    ],
  },
  {
    title: 'sidebar.groups.botConfig',
    items: [
      { icon: FileText, label: 'sidebar.menu.botMainConfig', path: '/config/bot', searchDescription: 'search.items.botConfigDesc' },
      { icon: Boxes, label: 'sidebar.menu.modelManagement', path: '/config/model', searchDescription: 'search.items.modelDesc', tourId: 'sidebar-model-management' },
      { icon: ScrollText, label: 'sidebar.menu.promptManagement', path: '/config/prompts' },
      { icon: Sparkles, label: 'sidebar.menu.promptGenerator', path: '/config/prompt-generator', searchDescription: 'search.items.promptGeneratorDesc' },
    ],
  },
  {
    title: 'sidebar.groups.botResources',
    items: [
      { icon: Smile, label: 'sidebar.menu.emojiManagement', path: '/resource/emoji', searchDescription: 'search.items.emojiDesc' },
      { icon: MessageSquare, label: 'sidebar.menu.expressionManagement', path: '/resource/expression', searchDescription: 'search.items.expressionDesc' },
      { icon: Hash, label: 'sidebar.menu.slangManagement', path: '/resource/jargon', searchDescription: 'search.items.jargonDesc' },
      { icon: BrainCircuit, label: 'sidebar.menu.behaviorLearning', path: '/resource/behavior', searchDescription: 'search.items.behaviorLearningDesc' },
      { icon: Database, label: 'sidebar.menu.knowledgeBase', path: '/resource/knowledge-base' },
    ],
  },
  {
    title: 'sidebar.groups.extensionsMonitor',
    items: [
      { icon: Sliders, label: 'sidebar.menu.pluginConfig', path: '/plugin-config' },
      { icon: Package, label: 'sidebar.menu.pluginMarket', path: '/plugins', searchDescription: 'search.items.pluginsDesc' },
      { icon: Network, label: 'sidebar.menu.mcpSettings', path: '/mcp-settings' },
    ],
  },
]
