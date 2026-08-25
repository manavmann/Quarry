-- Runner identity is the name a runner registers with: POST /api/runner/register
-- resolves a name to one runner_id, so names must be unique.
CREATE UNIQUE INDEX runners_name ON runners(name);
