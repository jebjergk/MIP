#!/usr/bin/env node
/**
 * Single source of truth: docs/UX_METRIC_GLOSSARY.yml
 * Generates:
 *   docs/UX_METRIC_GLOSSARY.json
 *   MIP/apps/mip_ui_web/src/data/UX_METRIC_GLOSSARY.json
 * Run from repo root: node MIP/apps/mip_ui_web/scripts/generate_glossary.js
 * Or from MIP/apps/mip_ui_web: node scripts/generate_glossary.js
 * With --check: exits 1 if generated JSON would differ from on-disk JSON (for CI/pre-commit).
 */

import { readFileSync, writeFileSync, existsSync } from 'fs'
import { dirname, join } from 'path'
import { fileURLToPath } from 'url'
import YAML from 'yaml'

const scriptDir = dirname(fileURLToPath(import.meta.url))
const appRoot = join(scriptDir, '..')
// Repo root is 3 levels up from mip_ui_web (MIP/apps/mip_ui_web -> MIP/apps -> MIP -> repo root)
const repoRoot = join(appRoot, '..', '..', '..')
const docsYml = join(repoRoot, 'docs', 'UX_METRIC_GLOSSARY.yml')
const docsJson = join(repoRoot, 'docs', 'UX_METRIC_GLOSSARY.json')
const appJson = join(appRoot, 'src', 'data', 'UX_METRIC_GLOSSARY.json')

function loadYaml() {
  const raw = readFileSync(docsYml, 'utf8')
  const parsed = YAML.parse(raw)
  if (!parsed || typeof parsed !== 'object') throw new Error('Invalid YAML or empty')
  return parsed
}

function toCanonicalJson(obj) {
  return JSON.stringify(obj, null, 2)
}

function main() {
  const checkOnly = process.argv.includes('--check')
  if (!existsSync(docsYml)) {
    console.error('Missing source:', docsYml)
    process.exit(1)
  }
  const data = loadYaml()
  const json = toCanonicalJson(data)
  if (checkOnly) {
    let failed = false
    for (const target of [docsJson, appJson]) {
      if (!existsSync(target)) {
        console.error('Missing expected file:', target)
        failed = true
        continue
      }
      const onDisk = readFileSync(target, 'utf8')
      if (onDisk.trim() !== json.trim()) {
        console.error('Generated JSON differs from', target)
        failed = true
      }
    }
    if (failed) {
      console.error('Run without --check to regenerate: node MIP/apps/mip_ui_web/scripts/generate_glossary.js')
      process.exit(1)
    }
    console.log('Glossary JSON is in sync with YAML.')
    return
  }
  writeFileSync(docsJson, json, 'utf8')
  console.log('Wrote', docsJson)
  writeFileSync(appJson, json, 'utf8')
  console.log('Wrote', appJson)
}

main()
