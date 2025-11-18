// https://nuxt.com/docs/api/configuration/nuxt-config
const baseURL = process.env.NUXT_APP_BASE_URL || '/';
const withBase = (path: string): string => {
  const normalizedBase = baseURL.endsWith('/') ? baseURL.slice(0, -1) : baseURL;
  const normalizedPath = path.startsWith('/') ? path : `/${path}`;
  return `${normalizedBase}${normalizedPath}`;
};

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
  app: {
    // Set via env NUXT_APP_BASE_URL="/xtraq/" for GitHub Pages; '/' for root hosting.
    baseURL,
    head: {
      link: [
        {
          rel: 'icon',
          type: 'image/svg+xml',
          href: withBase('/favicon.svg')
        }
      ]
    }
  },
  css: ['~/assets/css/main.css'],
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
          light: '/xtraq-logo.svg',
          dark: '/xtraq-logo.svg'
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
    },
    seo: {
      title: 'xtraq',
      description: 'Code generator for SQL Server stored procedures that creates strongly typed C# classes.'
    }
  },
  content: {
    build: {
      markdown: {
        highlight: {
          theme: {
            default: 'github-dark-default',
            light: 'github-light-default'
          },
          langs: ['bash', 'csharp', 'json', 'jsonc', 'powershell', 'sql']
        }
      }
    }
  },
  compatibilityDate: '2024-10-18',
  nitro: {
    prerender: {
      routes: ['/'],
      crawlLinks: true,
      autoSubfolderIndex: false
    }
  }
})
