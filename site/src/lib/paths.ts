import path from 'node:path';

/** The repository root. Builds run from `site/`, so the repo is one level up. */
export const REPO_ROOT = path.resolve(process.env.MINGA_REPO_ROOT ?? path.join(process.cwd(), '..'));
