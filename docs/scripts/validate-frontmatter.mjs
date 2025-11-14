#!/usr/bin/env node
import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'

const CONTENT_DIR = join(process.cwd(), 'content')
const ALLOWED_TAGS = new Set(['cli', 'build', 'generation', 'create', 'init', 'snapshot', 'sync'])
const REQUIRED_FIELDS = ['title', 'description']

let errors = 0

function walk(dir) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name)

    if (entry.isDirectory()) {
      walk(full)
    } else if (entry.name.endsWith('.md')) {
      validateFile(full)
    }
  }
}

function parseFrontmatter(raw) {
  if (!raw.startsWith('---')) {
    return null
  }

  const end = raw.indexOf('\n---', 3)

  if (end === -1) {
    return null
  }

  const header = raw.substring(3, end).trim()
  const body = raw.substring(end + 4)
  const obj = {}

  for (const line of header.split(/\r?\n/)) {
    const trimmed = line.trim()

    if (!trimmed || trimmed.startsWith('#')) {
      continue
    }

    const match = trimmed.match(/^([A-Za-z0-9_-]+):\s*(.*)$/)

    if (!match) {
      continue
    }

    let value = match[2].trim()

    if (value === 'true') {
      value = true
    } else if (value === 'false') {
      value = false
    } else if (/^\[.*\]$/.test(value)) {
      try {
        value = JSON.parse(value)
      } catch {
        // ignore invalid JSON arrays
      }
    }

    obj[match[1]] = value
  }

  return { data: obj, body }
}

function validateFile(file) {
  const raw = readFileSync(file, 'utf8')
  const fm = parseFrontmatter(raw)

  if (!fm) {
    console.warn(`[WARN] No frontmatter: ${file}`)
    return
  }

  const { data } = fm

  for (const field of REQUIRED_FIELDS) {
    if (data[field]) {
      continue
    }

    console.error(`[ERR] Missing required field '${field}' in ${file}`)
    errors += 1
  }

  if (!data.aiTags) {
    return
  }

  if (!Array.isArray(data.aiTags)) {
    console.error(`[ERR] aiTags must be array in ${file}`)
    errors += 1
    return
  }

  for (const tag of data.aiTags) {
    if (ALLOWED_TAGS.has(tag)) {
      continue
    }

    console.error(`[ERR] Unknown aiTag '${tag}' in ${file}`)
    errors += 1
  }
}

walk(CONTENT_DIR)

if (errors > 0) {
  console.error(`Validation failed with ${errors} error(s).`)
  process.exit(1)
} else {
  console.log('Frontmatter validation passed.')
}
