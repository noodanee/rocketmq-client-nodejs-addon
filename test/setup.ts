import * as path from 'path';

import { ensureBindingBinary } from './helpers/binding';

const rootDir = path.resolve(__dirname, '..');
ensureBindingBinary(rootDir);

process.env.NODE_BINDINGS_COMPILED_DIR = path.join(rootDir, 'build');
