export default defineAppConfig({
  ui: {
    colors: {
      primary: 'blue',
      neutral: 'slate'
    },
    footer: {
      slots: {
        root: 'border-t border-default',
        left: 'text-sm text-muted'
      }
    }
  },
  seo: {
    siteName: 'xtraq Documentation'
  },
  header: {
    title: 'xtraq',
    to: '/',
    logo: {
      alt: 'xtraq logo',
      light: '/xtraq-logo.svg',
      dark: '/xtraq-logo.svg'
    },
    search: true,
    colorMode: true,
    links: [{
      'icon': 'i-simple-icons-github',
      'to': 'https://github.com/nuetzliches/xtraq',
      'target': '_blank',
      'aria-label': 'GitHub'
    }]
  },
  footer: {
    logo: {
      alt: 'xtraq logo',
      light: '/xtraq-logo.svg',
      dark: '/xtraq-logo.svg'
    },
    credits: `© ${new Date().getFullYear()} xtraq`,
    colorMode: false,
    links: [{
      'icon': 'i-simple-icons-github',
      'to': 'https://github.com/nuetzliches/xtraq',
      'target': '_blank',
      'aria-label': 'xtraq on GitHub'
    }]
  },
  toc: {
    title: 'Table of Contents',
    bottom: {
      title: 'Community',
      edit: 'https://github.com/nuetzliches/xtraq/edit/main/docs/content',
      links: [{
        icon: 'i-lucide-star',
        label: 'Star on GitHub',
        to: 'https://github.com/nuetzliches/xtraq',
        target: '_blank'
      }, {
        icon: 'i-lucide-package',
        label: 'NuGet Package',
        to: 'https://www.nuget.org/packages/xtraq',
        target: '_blank'
      }]
    }
  }
})
