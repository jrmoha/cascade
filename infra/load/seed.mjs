// KAN-42 — seed the trimmed load stack with a project, an API key and the
// `level_complete` schema, then print ONLY the plaintext key to stdout so the
// caller can capture it:  API_KEY=$(node infra/load/seed.mjs)
//
// Diagnostics go to stderr so they never pollute the captured key.
const PS = process.env.PROJECT_SCHEMA_URL || 'http://localhost:3004';

async function post(path, body) {
  const res = await fetch(`${PS}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    throw new Error(`POST ${path} → ${res.status} ${await res.text()}`);
  }
  return res.status === 204 ? {} : res.json();
}

const project = await post('/projects', { name: 'load-test' });
console.error(`seeded project ${project.id}`);

const issued = await post(`/projects/${project.id}/keys`);
console.error('issued API key');

await post(`/projects/${project.id}/schemas`, {
  eventType: 'level_complete',
  jsonSchema: {
    type: 'object',
    properties: { level: { type: 'integer' }, score: { type: 'integer' } },
    required: ['level'],
    additionalProperties: true,
  },
});
console.error('registered level_complete schema');

process.stdout.write(issued.key);
