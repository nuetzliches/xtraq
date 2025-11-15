import { defineCollection, defineContentConfig } from '@nuxt/content'

export default defineContentConfig({
  navigation: {
    fields: ['navigation', 'description', 'icon', 'order']
  },
  collections: {
    docs: defineCollection({
      type: 'page',
      source: {
        include: '**'
      }
    })
  }
})
