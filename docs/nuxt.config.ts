// https://docus.dev/en/concepts/configuration

const lucideClientBundleIcons = [
  'lucide:alert-circle',
  'lucide:arrow-down',
  'lucide:arrow-left',
  'lucide:arrow-right',
  'lucide:arrow-up',
  'lucide:arrow-up-right',
  'lucide:book-open',
  'lucide:box',
  'lucide:circle-alert',
  'lucide:circle-check',
  'lucide:circle-question-mark',
  'lucide:circle-x',
  'lucide:chevron-down',
  'lucide:chevron-left',
  'lucide:chevron-right',
  'lucide:chevron-up',
  'lucide:chevrons-left',
  'lucide:chevrons-right',
  'lucide:code',
  'lucide:copy',
  'lucide:copy-check',
  'lucide:eye',
  'lucide:eye-off',
  'lucide:file',
  'lucide:folder',
  'lucide:folder-open',
  'lucide:hash',
  'lucide:info',
  'lucide:lightbulb',
  'lucide:link',
  'lucide:loader-circle',
  'lucide:menu',
  'lucide:minus',
  'lucide:monitor',
  'lucide:moon',
  'lucide:package',
  'lucide:panel-left-close',
  'lucide:panel-left-open',
  'lucide:pen',
  'lucide:plus',
  'lucide:rocket',
  'lucide:rotate-ccw',
  'lucide:search',
  'lucide:shield-check',
  'lucide:sliders-horizontal',
  'lucide:square',
  'lucide:star',
  'lucide:sun',
  'lucide:terminal',
  'lucide:triangle-alert',
  'lucide:upload',
  'lucide:wand-sparkles',
  'lucide:workflow',
  'lucide:x'
];

const simpleIconsClientBundleIcons = [
  'simple-icons:github'
];

export default defineNuxtConfig({
  extends: ['docus'],
  modules: ['@nuxt/eslint'],
  llms: {
    domain: 'https://nuetzliches.github.io/xtraq/',
    title: 'xtraq Documentation',
    description: 'Code generator for SQL Server stored procedures that creates strongly typed C# classes.',
    full: {
      title: 'xtraq Documentation',
      description: 'Code generator for SQL Server stored procedures that creates strongly typed C# classes.'
    }
  },
  eslint: {
    checker: false
  },
  css: ['~/assets/css/main.css'],
  icon: {
    provider: 'none',
    serverBundle: {
      collections: ['lucide', 'simple-icons']
    },
    clientBundle: {
      icons: [
        ...lucideClientBundleIcons, 
        ...simpleIconsClientBundleIcons
      ],
      scan: true
    },
    fallbackToApi: false
  },
  content: {
    highlight: {
      preload: ['csharp'],
      langs: [
        {
          name: 'csharp',
          alias: ['cs']
        }
      ]
    }
  },
  appConfig: {
    docus: {
      title: 'xtraq Documentation',
      description: 'Code generator for SQL Server stored procedures that creates strongly typed C# classes.',
      url: 'https://nuetzliches.github.io/xtraq/',
      socials: {
        github: 'nuetzliches/xtraq',
        nuget: {
          label: 'NuGet',
          icon: 'i-lucide-package',
          href: 'https://www.nuget.org/packages/xtraq'
        }
      },
      github: {
        repo: 'nuetzliches/xtraq',
        branch: 'master',
        dir: 'docs/content',
        edit: true,
        releases: true
      },
      header: {
        title: 'xtraq',
        logo: {
          light: 'xtraq-logo.svg',
          dark: 'xtraq-logo.svg'
        },
        showLinkIcon: true,
        actions: [
          {
            label: 'GitHub',
            to: 'https://github.com/nuetzliches/xtraq',
            icon: 'i-simple-icons-github',
            target: '_blank'
          }
        ]
      },
      aside: {
        level: 1,
        collapsed: false
      },
      main: {
        fluid: false,
        padded: true,
      },
      footer: {
        credits: {
          text: `© ${new Date().getFullYear()} xtraq`
        },
        navigation: true,
        textLinks: [
          {
            icon: 'i-lucide-star',
            text: 'Star on GitHub',
            href: 'https://github.com/nuetzliches/xtraq'
          },
          {
            icon: 'i-lucide-package',
            text: 'NuGet Package',
            href: 'https://www.nuget.org/packages/xtraq'
          }
        ]
      },
      toc: {
        title: 'On this page'
      }
    }
  }
})
