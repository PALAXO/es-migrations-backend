import { defineConfig } from 'eslint/config';
import promise from 'eslint-plugin-promise';
import jsdoc from 'eslint-plugin-jsdoc';
import stylisticJs from '@stylistic/eslint-plugin-js';
import globals from 'globals';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import js from '@eslint/js';
import { FlatCompat } from '@eslint/eslintrc';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const compat = new FlatCompat({
    baseDirectory: __dirname,
    recommendedConfig: js.configs.recommended,
    allConfig: js.configs.all
});

export default defineConfig([
    {
        extends: compat.extends(`eslint:recommended`, `plugin:promise/recommended`),

        plugins: {
            promise,
            jsdoc,
            '@stylistic/js': stylisticJs
        },

        languageOptions: {
            globals: {
                ...globals.node,
            },

            ecmaVersion: `latest`,
            sourceType: `module`,
        },

        rules: {
            '@stylistic/js/indent': [`error`, 4, {
                SwitchCase: 1,
            }],
            '@stylistic/js/object-curly-spacing': [`error`, `always`],
            '@stylistic/js/quotes': [`error`, `backtick`],
            '@stylistic/js/semi': [`error`, `always`],
            'no-var': [`error`],
            eqeqeq: [`error`],
            '@stylistic/js/arrow-parens': [`error`, `always`],
            'no-async-promise-executor': [`off`],
            'no-prototype-builtins': [`off`],
            'no-unused-vars': [`error`, {
                args: `after-used`,
                caughtErrors: `none`
            }],
            'prefer-const': [`error`],
            'prefer-template': [`error`],
            'promise/always-return': [`off`],
            'promise/no-nesting': [`off`],
            'promise/no-callback-in-promise': [`off`],
            'jsdoc/require-returns': [`warn`]
        }
    }
]);
