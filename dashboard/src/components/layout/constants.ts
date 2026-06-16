import {
  RiBox3Fill,
  RiBrainFill,
  RiCodeSSlashFill,
  RiDatabase2Fill,
  RiEmotionFill,
  RiFileSettingsFill,
  RiFileTextFill,
  RiHomeFill,
  RiMessage3Fill,
  RiNetworkFill,
  RiPulseFill,
  RiPuzzleFill,
  RiStoreFill,
} from '@remixicon/react'

import type { MenuSection } from './types'

export const menuSections: MenuSection[] = [
  {
    title: 'sidebar.groups.overview',
    items: [
      { icon: RiHomeFill, label: 'sidebar.menu.home', path: '/', searchDescription: 'search.items.homeDesc' },
      { icon: RiPulseFill, label: 'sidebar.menu.maisakaMonitor', path: '/planner-monitor' },
    ],
  },
  {
    title: 'sidebar.groups.botConfig',
    items: [
      { icon: RiFileSettingsFill, label: 'sidebar.menu.botMainConfig', path: '/config/bot', searchDescription: 'search.items.botConfigDesc' },
      { icon: RiBox3Fill, label: 'sidebar.menu.modelManagement', path: '/config/model', searchDescription: 'search.items.modelDesc', tourId: 'sidebar-model-management' },
      { icon: RiFileTextFill, label: 'sidebar.menu.promptManagement', path: '/config/prompts' },
    ],
  },
  {
    title: 'sidebar.groups.botResources',
    items: [
      { icon: RiEmotionFill, label: 'sidebar.menu.emojiManagement', path: '/resource/emoji', searchDescription: 'search.items.emojiDesc' },
      { icon: RiMessage3Fill, label: 'sidebar.menu.expressionManagement', path: '/resource/expression', searchDescription: 'search.items.expressionDesc' },
      { icon: RiCodeSSlashFill, label: 'sidebar.menu.slangManagement', path: '/resource/jargon', searchDescription: 'search.items.jargonDesc' },
      { icon: RiBrainFill, label: 'sidebar.menu.behaviorLearning', path: '/resource/behavior', searchDescription: 'search.items.behaviorLearningDesc', featureFlag: 'behaviorLearning' },
      { icon: RiDatabase2Fill, label: 'sidebar.menu.knowledgeBase', path: '/resource/knowledge-base' },
    ],
  },
  {
    title: 'sidebar.groups.extensionsMonitor',
    items: [
      { icon: RiPuzzleFill, label: 'sidebar.menu.pluginConfig', path: '/plugin-config' },
      { icon: RiStoreFill, label: 'sidebar.menu.pluginMarket', path: '/plugins', searchDescription: 'search.items.pluginsDesc' },
      { icon: RiNetworkFill, label: 'sidebar.menu.mcpSettings', path: '/mcp-settings' },
    ],
  },
]
