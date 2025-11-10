// https://nuxt.com/docs/api/configuration/nuxt-config
export default defineNuxtConfig({
  modules: [
    '@nuxt/eslint',
    '@nuxt/image',
    '@nuxt/ui',
    '@nuxt/content',
    'nuxt-og-image',
    'nuxt-llms'
  ],
  devtools: { enabled: true },
  app: {
    // Set via env NUXT_APP_BASE_URL="/xtraq/" for GitHub Pages; '/' for root hosting.
    baseURL: process.env.NUXT_APP_BASE_URL || '/'
  },

  css: ['~/assets/css/main.css'],

  content: {
    build: {
      markdown: {
        toc: {
          searchDepth: 1
        }
      }
    }
  },

  compatibilityDate: '2024-07-11',

  nitro: {
    prerender: {
      routes: ['/'],
      crawlLinks: true,
      autoSubfolderIndex: false
    }
  },

  eslint: {
    config: {
      stylistic: {
        commaDangle: 'never',
        braceStyle: '1tbs'
      }
    }
  },

  icon: {
    provider: 'iconify'
  },

  llms: {
    domain: 'https://xtraq.dev/',
    title: 'xtraq Documentation',
    description: 'Code generator for SQL Server stored procedures that creates strongly typed C# classes.',
    full: {
      title: 'xtraq Documentation - Complete Reference',
      description: 'Complete documentation for xtraq, the SQL Server stored procedure code generator for .NET applications.'
    },
    sections: []
  }
})
