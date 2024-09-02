'use strict';

function exportSpans(instanceId, databaseId, projectId) {
  // [START spanner_export_traces]
  // Imports the Google Cloud client library and OpenTelemetry libraries for exporting traces.
  const {Resource} = require('@opentelemetry/resources');
  const {NodeSDK} = require('@opentelemetry/sdk-node');
  const {
    NodeTracerProvider,
    TraceIdRatioBasedSampler,
  } = require('@opentelemetry/sdk-trace-node');
  const {
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
  } = require('@opentelemetry/sdk-trace-base');
  const {
    SEMRESATTRS_SERVICE_NAME,
    SEMRESATTRS_SERVICE_VERSION,
  } = require('@opentelemetry/semantic-conventions');

  const resource = Resource.default().merge(
    new Resource({
      [SEMRESATTRS_SERVICE_NAME]: 'spanner-sample',
      [SEMRESATTRS_SERVICE_VERSION]: 'v1.0.0', // Whatever version of your app is running.,
    })
  );

  const options = {serviceName: 'nodejs-spanner'};
  const {
    TraceExporter,
  } = require('@google-cloud/opentelemetry-cloud-trace-exporter');
  const exporter = new TraceExporter();

  const sdk = new NodeSDK({
    resource: resource,
    traceExporter: exporter,
    // Trace every single request to ensure that we generate
    // enough traffic for proper examination of traces.
    sampler: new TraceIdRatioBasedSampler(1.0),
  });
  sdk.start();

  const provider = new NodeTracerProvider({resource: resource});
  provider.addSpanProcessor(new BatchSpanProcessor(exporter));
  provider.register();

  const tracer = provider.getTracer(
    'cloud.google.com/nodejs/spanner',
    '7.14.0'
  );
  tracer.startActiveSpan('deleteAndCreateDatabase', async span => {
    const {Spanner} = require('@google-cloud/spanner');

    // Creates a client
    const spanner = new Spanner({
      projectId: projectId,
      observability: {
        tracerProvider: provider,
        enableExtendedTracing: true,
      },
    });

    // Gets a reference to a Cloud Spanner instance and database
    const instance = spanner.instance(instanceId);
    const database = instance.database(databaseId);
    const databaseAdminClient = spanner.getDatabaseAdminClient();

    const databasePath = databaseAdminClient.databasePath(
      projectId,
      instanceId,
      databaseId
    );

    // await new Promise((resolve, reject) => setTimeout(resolve, 400));

    deleteDatabase(databaseAdminClient, databasePath, () => {
      createDatabase(
        databaseAdminClient,
        projectId,
        instanceId,
        databaseId,
        () => {
          insertUsingDml(tracer, database, async () => {
            try {
              const query = {
                sql: 'SELECT SingerId, FirstName, LastName FROM Singers',
              };
              const [rows] = await database.run(query);

              for (const row of rows) {
                const json = row.toJSON();

                console.log(
                  `SingerId: ${json.SingerId}, FirstName: ${json.FirstName}, LastName: ${json.LastName}`
                );
              }
            } catch (err) {
              console.error('ERROR:', err);
              await new Promise((resolve, reject) => setTimeout(resolve, 2000));
            } finally {
              span.end();
              spanner.close();
              console.log('main span.end');
            }

            await new Promise((resolve, reject) => {
              setTimeout(() => {
                console.log('finished delete and creation of the database');
              }, 8800);
            });
          });
        }
      );
    });
  });

  // [END spanner_export_traces]
}

function quickstart() {}

function insertUsingDml(tracer, database, callback) {
  tracer.startActiveSpan('insertUsingDML', span => {
    database.runTransaction(async (err, transaction) => {
      if (err) {
        span.end();
        console.error(err);
        return;
      }

      try {
        const [delCount] = await transaction.runUpdate({
          sql: 'DELETE FROM Singers WHERE 1=1',
        });

        console.log(`Deletion count ${delCount}`);

        const [rowCount] = await transaction.runUpdate({
          sql: 'INSERT Singers (SingerId, FirstName, LastName) VALUES (10, @firstName, @lastName)',
          params: {
            firstName: 'Virginia',
            lastName: 'Watson',
          },
        });

        console.log(
          `Successfully inserted ${rowCount} record into the Singers table.`
        );

        await transaction.commit();
      } catch (err) {
        console.error('ERROR:', err);
      } finally {
        // Close the database when finished.
        console.log('exiting insertUsingDml');
        tracer.startActiveSpan('timingOutToExport-insertUsingDML', eSpan => {
          setTimeout(() => {
            if (callback) {
              callback();
            }
            eSpan.end();
            span.end();
          }, 500);
        });
      }
    });
  });
}

function createDatabase(
  databaseAdminClient,
  projectId,
  instanceId,
  databaseId,
  callback
) {
  async function doCreateDatabase() {
    if (databaseId) {
      callback();
      return;
    }

    // Create the database with default tables.
    const createSingersTableStatement = `
      CREATE TABLE Singers (
        SingerId   INT64 NOT NULL,
        FirstName  STRING(1024),
        LastName   STRING(1024),
        SingerInfo BYTES(MAX)
      ) PRIMARY KEY (SingerId)`;
    const createAlbumsStatement = `
      CREATE TABLE Albums (
        SingerId     INT64 NOT NULL,
        AlbumId      INT64 NOT NULL,
        AlbumTitle   STRING(MAX)
      ) PRIMARY KEY (SingerId, AlbumId),
        INTERLEAVE IN PARENT Singers ON DELETE CASCADE`;

    const [operation] = await databaseAdminClient.createDatabase({
      createStatement: 'CREATE DATABASE `' + databaseId + '`',
      extraStatements: [createSingersTableStatement, createAlbumsStatement],
      parent: databaseAdminClient.instancePath(projectId, instanceId),
    });

    console.log(`Waiting for creation of ${databaseId} to complete...`);
    await operation.promise();
    console.log(`Created database ${databaseId}`);
    callback();
  }
  doCreateDatabase();
}

function deleteDatabase(databaseAdminClient, databasePath, callback) {
  async function doDropDatabase() {
    if (databasePath) {
      callback();
      return;
    }

    const [operation] = await databaseAdminClient.dropDatabase({
      database: databasePath,
    });

    await operation;
    console.log('Finished dropping the database');
    callback();
  }

  doDropDatabase();
}

require('yargs')
  .demand(1)
  .command(
    'export <instanceName> <databaseName> <projectId>',
    'Execute a read-only transaction on an example Cloud Spanner table.',
    {},
    opts => exportSpans(opts.instanceName, opts.databaseName, opts.projectId)
  )
  .example('node $0 export "my-instance" "my-database" "my-project-id"')
  .wrap(120)
  .recommendCommands()
  .epilogue('For more information, see https://cloud.google.com/spanner/docs')
  .strict()
  .help().argv;
