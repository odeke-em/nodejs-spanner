/*!
 * Copyright 2024 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const {MutationSet, Spanner} = require('@google-cloud/spanner');
const {readFileSync} = require('fs');

async function runComparisons(v1JSONFile, v2JSONFile) {
  const v1JSON = JSON.parse(readFileSync(v1JSONFile));
  const v2JSON = JSON.parse(readFileSync(v2JSONFile));
  return compareDifferences(v1JSON, v2JSON);
}

function main() {
  if (process.argv[2] === 'compare') {
    runComparisons(...process.argv.slice(3));
  } else {
    runBenchmarking(...process.argv.slice(2));
  }
}

async function runBenchmarking(projectId, instanceId, databaseId) {
  // Otherwise run the benchmarks.
  const spanner = new Spanner({
    projectId: projectId,
    observabilityOptions: {
      enableExtendedTracing: true,
    },
  });

  const instance = spanner.instance(instanceId);
  const database = instance.database(databaseId);

  const runners = [
    databaseWriteAtLeastOnce,
    dqlSelect1,
    dqlSelectWithSyntaxError,
    dmlDeleteThenInsert,
    dmlWithDuplicates,
    databasBatcheCreateSessions,
    databasGetSessions,
    databaseRun,
    databaseRunWithSyntaxError,
    databaseRunWithDelete,
    databaseRunWithDeleteFromNonExistentTable,
  ]; 

  const nRuns = 1000;
  const benchmarkValues = {};

  let k = 0;
  for (k=0; k < runners.length; k++){
    const fn = runners[k];
    const method = fn.name;
    console.log(`Running ${k+1}/${runners.length} ${method}`);
    const latencyL = [];
    const ramL = [];
    let i = 0;

    for (i = 0; i < nRuns; i++) {
        const startTime = process.hrtime.bigint();
        const startHeapUsedBytes = process.memoryUsage().heapUsed;
        try {
          await fn(database);
        } catch (e) {
        } finally {
          latencyL.push(process.hrtime.bigint() - startTime);
          ramL.push(process.memoryUsage().heapUsed - startHeapUsedBytes);
      }
    }

      const lessComparator = (a, b) => {
        if (a < b) return -1;
        if (a > b) return 1;
        return 0;
      };
      latencyL.sort(lessComparator);
      ramL.sort(lessComparator);

      benchmarkValues[method] = {
          ram: percentiles(method, ramL, 'bytes'),
          latency:  percentiles(method, latencyL, 'time'),
      };
  }

  BigInt.prototype.toJSON = function () {
    return Number(this);
  };
  console.log(JSON.stringify(benchmarkValues));
}

function percentiles(method, sortedValues, kind) {
  const n = sortedValues.length;
  const p50 = sortedValues[Math.floor(n * 0.5)];
  const p75 = sortedValues[Math.floor(n * 0.75)];
  const p90 = sortedValues[Math.floor(n * 0.90)];
  const p95 = sortedValues[Math.floor(n * 0.95)];
  const p99 = sortedValues[Math.floor(n * 0.99)];
  // console.log(
  //   `\tp50: ${p50}\n\tp75: ${p75}\n\tp95: ${p95}\n\tp99: ${p99}\n`
  // );
  return {
      p50: p50, p75: p75, p90: p90, p95: p95, p99:p99,
      p50_s: humanize(p50, kind),
      p75_s: humanize(p75, kind),
      p90_s: humanize(p90, kind),
      p95_s: humanize(p95, kind),
      p99_s: humanize(p99, kind),
  };
}

function humanize(values, kind) {
    let converterFn = humanizeTime;
    if (kind === 'bytes') {
        converterFn = humanizeBytes;
    }
    return converterFn(values);
}

const secondUnits = ['ns', 'us', 'ms', 's'];
const pastSecondUnits = [['min', 60], ['hr', 60], ['day', 24], ['week', 7], ['month', 30]];
function humanizeTime(ns) {
  let value = ns;
  for (const unit of secondUnits) {
    if (value < 1000) {
        return `${value} ${unit}`;
    }
    value /= 1000n;
  }
  
  for (const unitPlusValue of pastSecondUnits) {
    const [unitName, divisor] = unitPlusValue;
    if (value < divisor) {
        return `${value} ${unitName}`;
    }
    value = value/divisor;
  }
  return `${value} ${units[units.length-1][0]}`;
}

const bytesUnits = ['B', 'kB', 'MB', 'GB', 'TB', 'PB', 'ExB'];
function humanizeBytes(b) {
  let value = b;
  for (const unit of bytesUnits) {
    if (value < 1024) {
      return `${value.toFixed(3)} ${unit}`;
    }
    value = value/1024;
  }

  return `${value.toFixed(3)} ${bytesUnits[bytesUnits.length-1]}`;
}

async function dqlSelect1(database) {
  const [snapshot] = await database.getSnapshot();
  const [rows] = await snapshot.run('SELECT 1');
  await snapshot.end();
}

async function dqlSelectWithSyntaxError(database) {
  const [snapshot] = await database.getSnapshot();
  try {
    const [rows] = await snapshot.run('SELECT 1');
  } finally {
    await snapshot.end();
  }
}

async function dmlDeleteThenInsert(database) {
  await database.runTransactionAsync(async tx => {
    const [updateCount1] = await tx.runUpdate('DELETE FROM Singers WHERE 1=1');
    const [updateCount2] = await tx.runUpdate(
      "INSERT INTO Singers(SingerId, firstName) VALUES(1, 'DTB')"
    );
    await tx.commit();
  });
}

async function dmlWithDuplicates(database) {
  return await database.runTransactionAsync(async tx => {
    try {
    const [updateCount1] = await tx.runUpdate(
      "INSERT INTO Singers(SingerId, firstName) VALUES(1, 'DTB')"
    );
    const [updateCount2] = await tx.runUpdate(
      "INSERT INTO Singers(SingerId, firstName) VALUES(1, 'DTB')"
    );
    } catch(e) {
    } finally {
      await tx.end();
    }
  });
}

async function databasBatcheCreateSessions(database) {
  return await database.batchCreateSessions(10);
}

async function databasGetSessions(database) {
  return await database.getSessions();
}

async function databaseRun(database) {
  return await database.run('SELECT 1');
}

async function databaseRunWithSyntaxError(database) {
  return await database.run('SELECT 10 p');
}

async function databaseRunWithDelete(database) {
  return await database.run('DELETE FROM Singers WHERE 1=1');
}

async function databaseRunWithDeleteFromNonExistentTable(database) {
  return await database.run('DELETE FROM NonExistent WHERE 1=1');
}

async function databaseWriteAtLeastOnce(database) {
  const mutations = new MutationSet();
  mutations.upsert('Singers', {
   SingerId: 1,
   FirstName: 'Scarlet',
   LastName: 'Terry',
  });
  mutations.upsert('Singers', {
   SingerId: 2,
   FirstName: 'Marc',
   LastName: 'Richards',
  });

  const [response, err] = await database.writeAtLeastOnce(mutations, {});
} 

function compareDifferences(v1, v2) {
    const percents = [];
    for (const key in v1) {
        const defV1 = v1[key];
        const ramV1 = defV1.ram;
        const latencyV1 = defV1.latency;
        const defV2 = v2[key];
        const ramV2 = defV2.ram;
        const latencyV2 = defV2.latency;

        percents.push({
            key: key,
            ramP50:calculatePercent(ramV1.p50, ramV2.p50).toFixed(2),
            ramP75:calculatePercent(ramV1.p75, ramV2.p75).toFixed(2),
            ramP90:calculatePercent(ramV1.p90, ramV2.p90).toFixed(2),
            ramP95:calculatePercent(ramV1.p95, ramV2.p95).toFixed(2),
            ramP99:calculatePercent(ramV1.p95, ramV2.p99).toFixed(2),
            latP50: calculatePercent(latencyV1.p50, latencyV2.p50).toFixed(2),
            latP75: calculatePercent(latencyV1.p75, latencyV2.p75).toFixed(2),
            latP90: calculatePercent(latencyV1.p90, latencyV2.p90).toFixed(2),
            latP95: calculatePercent(latencyV1.p95, latencyV2.p95).toFixed(2),
            latP99: calculatePercent(latencyV1.p95, latencyV2.p99).toFixed(2),
        });
    }

    // Deterministically print out results sorted by name.
    percents.sort((a, b) => {
        if (a.key < b.key) return -1;
        if (a.key > b.key) return +1;
        return 0;
    });

    for (const value of percents) {
        console.log(`${value.key}`);
        console.log(`\t      p50 (%)  p75 (%)  p90 (%)  p95 (%)  p99 (%)`);
        console.log(`\tRAM: ${value.ramP50}    ${value.ramP75}      ${value.ramP90}   ${value.ramP95}   ${value.ramP99}`);
        console.log(`\tLat: ${value.latP50}    ${value.latP75}      ${value.latP90}   ${value.latP95}   ${value.latP99}`);
        console.log('');
    }
}

function calculatePercent(orig, updated) {
  return (updated-orig)*100/orig;
}

process.on('unhandledRejection', err => {
  console.error(err.message);
  process.exitCode = 1;
});
main();
