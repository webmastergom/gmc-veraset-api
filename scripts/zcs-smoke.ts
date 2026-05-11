/**
 * SMOKE TEST — runs the ZCS multi-phase state machine end-to-end against
 * REAL Athena/S3 with a small megajob (FR Siblu, 29 POIs, 2 months).
 *
 * Run: npx tsx _smoke-zcs.ts
 *
 * Test cases:
 *   1. BASIC megajob (FR - 29 Siblu - Mar 2026, schema=BASIC)
 *   2. FULL megajob (France Grid Abril 2025, schema=FULL)
 *
 * Each one walks the state machine phase-by-phase, logging every
 * transition. If a phase throws, the script prints the full error and
 * exits with code 1.
 */
import 'dotenv/config';
import {
  type ZcsState,
  type ZcsRunConfig,
  phaseStarting,
  phasePrepareTable,
  phaseLaunchQueries,
  phasePollingQueries,
  phaseAggregateFull,
  phasePass1Basic,
  phaseGeocoding,
  phasePass2Basic,
} from './lib/zcs-multiphase';

const CASES: Array<{ name: string; cfg: ZcsRunConfig }> = [
  {
    name: 'BASIC: FR - 29 Siblu (small, 29 POIs, 2 months)',
    cfg: {
      megaJobId: 'eb4c97eb-62f4-4a39-b7c5-1283ca6aac43',
      country: 'FR',
      postalCodes: ['75007', '75008', '75016', '06400', '06500'],
    },
  },
  {
    name: 'FULL: France Grid Abril 2025 (large, FAST path)',
    cfg: {
      megaJobId: '32f1120e-72e0-46f5-8f95-54aec4c51f1c',
      country: 'FR',
      postalCodes: ['75007', '75008', '75016', '92100', '92200'],
    },
  },
];

async function runOne(state: ZcsState) {
  let iters = 0;
  while (state.phase !== 'done' && state.phase !== 'error' && iters < 50) {
    const before = state.phase;
    const t0 = Date.now();
    try {
      switch (state.phase) {
        case 'starting':
          state = await phaseStarting(state); break;
        case 'prepare_table':
          state = await phasePrepareTable(state); break;
        case 'launch_queries':
          state = await phaseLaunchQueries(state); break;
        case 'polling_queries':
          state = await phasePollingQueries(state); break;
        case 'aggregate_full':
          state = await phaseAggregateFull(state); break;
        case 'pass1_basic':
          state = await phasePass1Basic(state); break;
        case 'geocoding':
          state = await phaseGeocoding(state); break;
        case 'pass2_basic':
          state = await phasePass2Basic(state); break;
      }
    } catch (e: any) {
      console.error(`  ❌ ${before} threw after ${Date.now() - t0}ms:`);
      console.error(`     ${e.message}`);
      if (e.stack) console.error(e.stack.split('\n').slice(1, 4).join('\n'));
      return { ok: false, state, error: e.message };
    }
    const dt = Date.now() - t0;
    if (before === state.phase) {
      // polling_queries — still waiting; print and sleep
      process.stdout.write(`  ⏳ ${before} (${dt}ms): ${state.subProgress?.label}\n`);
      await new Promise((r) => setTimeout(r, 3000));
    } else {
      console.log(`  ✅ ${before} → ${state.phase} (${dt}ms): ${state.subProgress?.label}`);
    }
    iters++;
  }
  if (iters >= 50) {
    console.error('  ❌ Iteration cap hit (50) — polling never progressed');
    return { ok: false, state };
  }
  return { ok: state.phase === 'done', state };
}

async function main() {
  for (const c of CASES) {
    console.log(`\n=== ${c.name} ===`);
    const initial: ZcsState = {
      phase: 'starting',
      runId: 'smoke-' + Math.random().toString(36).slice(2, 10),
      config: c.cfg,
      sourceLabel: c.cfg.megaJobId
        ? `megajob:${c.cfg.megaJobId}`
        : c.cfg.datasetName!,
      updatedAt: new Date().toISOString(),
    };
    const t0 = Date.now();
    const { ok, state, error } = await runOne(initial);
    const dt = Date.now() - t0;
    if (ok) {
      console.log(`\n  🎯 DONE in ${(dt / 1000).toFixed(1)}s — resultKey=${state.resultKey}`);
    } else {
      console.error(`\n  💥 FAILED after ${(dt / 1000).toFixed(1)}s — phase=${state.phase}`);
      if (error) console.error(`     error: ${error}`);
      process.exit(1);
    }
  }
  console.log('\n✅ ALL SMOKE TESTS PASSED');
}

main().catch((e) => {
  console.error('Fatal:', e);
  process.exit(1);
});
