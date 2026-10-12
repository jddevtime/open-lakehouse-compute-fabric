# Vendored web assets

Everything the browser loads is served from this directory. There is no CDN, no remote font and no network access at run time. Each library is copied unmodified from its npm release unless the notes say otherwise.

## Libraries

| Library | Version | Licence | Source archive (sha256) | Notes |
|---|---|---|---|---|
| codemirror | 5.65.20 | MIT | `codemirror-5.65.20.tgz` `0ef6484ea4f71c28c6b3dd146349e79786762fc4b42e5b33befea6201c7fa6c8` | Code editor (core, python/sql/markdown/javascript/clike/shell/yaml/xml modes, comment, placeholder, closebrackets, matchbrackets, show-hint, sql-hint, overlay, active-line addons). Unmodified. |
| fonts | 5.3.0 | OFL-1.1 | `fontsource-variable-jetbrains-mono-5.3.0.tgz` `996fe6368a480c9ce15d4de22a2682b7c40b403718fee2b15e7272f244fd993f`; `fontsource-variable-schibsted-grotesk-5.3.0.tgz` `dd729607754cfbf3f4c7f0872ba6d727e953a3d2b533184afe009b134f04f996` | JetBrains Mono and Schibsted Grotesk variable fonts, latin subset, woff2. Self-hosted, never loaded from a CDN. |
| htm | 3.1.1 | Apache-2.0 | `htm-3.1.1.tgz` `2425b9bee11409177bcabc7f32e319926fc6690c1701c0b257c88bdff2d5ba90` | Tagged-template JSX alternative (module build). Unmodified. |
| preact | 10.29.8 | MIT | `preact-10.29.8.tgz` `b18cb0a457f3d43c7bb30391a74ade7d13e03bc6e77915e061c70c0fe1123299` | UI runtime. hooks.module.js has one change: the bare import "preact" is rewritten to "./preact.module.js" so it resolves without an import map. The sourceMappingURL comments are stripped. |
| swagger-ui | 5.33.1 | Apache-2.0 | `swagger-ui-dist-5.33.1.tgz` `b468ff5f49451f194a739bc245c85b28c7d3d33057a8409a9b2369da37e215b9` | Interactive API reference served at /v1/docs (bundle, css, LICENSE, NOTICE). Unmodified. |

Licence texts sit next to the files: `codemirror/LICENSE`, `fonts/LICENSE-*.txt`, `htm/LICENSE`, `preact/LICENSE`, `swagger-ui/LICENSE`, `swagger-ui/NOTICE`, `swagger-ui/swagger-ui-bundle.js.LICENSE.txt`.

## File checksums (sha256)

| File | Library | Licence | sha256 |
|---|---|---|---|
| codemirror/LICENSE | codemirror | MIT | `168a4becc968f5001e2ee2e0291b6e4daabafc1894a11ade1e11d56e96096e07` |
| codemirror/addon/comment/comment.js | codemirror | MIT | `a65c038258c6541658a0e9f24c56c78255e0e20d4cf06aa9ad83342069a589be` |
| codemirror/addon/display/placeholder.js | codemirror | MIT | `44619626e2301fc16ebade252564e2c064113483e02f2fc8d8d262aed2485b9c` |
| codemirror/addon/edit/closebrackets.js | codemirror | MIT | `143c3014c29254f3531cc30be6d90205084bcfc36cffa6f9b2a46fd42a40be20` |
| codemirror/addon/edit/matchbrackets.js | codemirror | MIT | `9d23a177f1e4d07e3acc37c210ffbe4c1c2eb4d55bbe54c16d3e7ad3d04d0401` |
| codemirror/addon/hint/show-hint.css | codemirror | MIT | `9058c1c14fcdae199b490bb6214f36a216b9ce84d7df2084830ebb6a60337651` |
| codemirror/addon/hint/show-hint.js | codemirror | MIT | `6d940e45a07c13abd1e872802ba20f91f809d4b583afcd58a8058e25c7f0cfce` |
| codemirror/addon/hint/sql-hint.js | codemirror | MIT | `8723e07aa931aafc160404ec65f0cc05b9cb44198edb3d0e61efb5c3f83d202a` |
| codemirror/addon/mode/overlay.js | codemirror | MIT | `1b44334e2a04d90f2b473c26847798859039190142e53f72f3b7de0f82696d0e` |
| codemirror/addon/selection/active-line.js | codemirror | MIT | `3afbcf78835c9bdc342e3992c53d9b74286722613bb1a8adc9d140edc8737ee8` |
| codemirror/codemirror.css | codemirror | MIT | `eb494ea972d2661ef86f7f6ac656dd6786d721e49c9c1b46e1eb967e4b6f9bf3` |
| codemirror/codemirror.js | codemirror | MIT | `8e8b5e3cc4be7530a9b6b624bce95ff966cd667283005a103739fe37d3bdd97c` |
| codemirror/mode/clike/clike.js | codemirror | MIT | `25c92495700d1883b5f0e08f2e77d6fe0265f992e47b59199aa8746f66c4021b` |
| codemirror/mode/javascript/javascript.js | codemirror | MIT | `1311c73c66308ba6f78512b4c2e770a6900c80c3629683763329668ee6111163` |
| codemirror/mode/markdown/markdown.js | codemirror | MIT | `8fe7b11848f79cc9119b2a239f38a00944f6f2bb764a00bc837ecf8d34cf7690` |
| codemirror/mode/python/python.js | codemirror | MIT | `19a59ca387addb04e95002c9adbe2b8c231427ce49369ac537107e3088a6947c` |
| codemirror/mode/shell/shell.js | codemirror | MIT | `24f77cb162ea9d9e9fc79b95ba547a7cc10a0767e3a5a52c786d4c24253736fe` |
| codemirror/mode/sql/sql.js | codemirror | MIT | `d5edb6bd39caa6155cab6bcdeb380a29229264a8a26e5fddd21ca12f0ebf359f` |
| codemirror/mode/xml/xml.js | codemirror | MIT | `dc7e44d410399326f802e2924573cbf6f942a79f647fd0b97f0b607973bc9a09` |
| codemirror/mode/yaml/yaml.js | codemirror | MIT | `7de73109e5bfb6951d53764f5210f00f7859b57525811fab6b6f843980a7726e` |
| fonts/LICENSE-jetbrains-mono.txt | fonts | OFL-1.1 | `403581b69dac5cff4079205e01c6b467e56af449ecbd7247693ddb1baafa005b` |
| fonts/LICENSE-schibsted-grotesk.txt | fonts | OFL-1.1 | `15222412a95d70042e66e79bfb3b985653e0693e4d41703bc131bee9c5aa1aa7` |
| fonts/jetbrains-mono-latin-wght-normal.woff2 | fonts | OFL-1.1 | `18be452724bfdc236c074ca94a249a7f41a86752c7d04ab258ce9ed5651f6a7e` |
| fonts/schibsted-grotesk-latin-wght-normal.woff2 | fonts | OFL-1.1 | `4c8b93f431d462c696e12b9d6a033feb3394d36e66e781357c496b95d8a75e05` |
| htm/LICENSE | htm | Apache-2.0 | `740725f7252e750af735d0028cc534970772f513331e9f68150fede8fb3ce00f` |
| htm/htm.module.js | htm | Apache-2.0 | `ab33dd3f38059b9be4d5f5350128eefb2356639c4e0bbe9d9e8b3ba75847e9e4` |
| preact/LICENSE | preact | MIT | `1fe6958409c8c257a70c587a18b6f7f412b179b456630790d30b2ec9a8e4b7d4` |
| preact/hooks.module.js | preact | MIT | `8e0e4efa954c9306af73bd0e4f82b713f6c7d5309c152306af94a68af004ce7b` |
| preact/preact.module.js | preact | MIT | `712517dd2a42c3c22b5ab0264ad7e9a2b1e8be6cf97fa00dbedd98203617e616` |
| swagger-ui/LICENSE | swagger-ui | Apache-2.0 | `cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30` |
| swagger-ui/NOTICE | swagger-ui | Apache-2.0 | `0d20d1adef18aee3f40dd258172155521ce702ac445cb5f7b7d60ed32dad2fb2` |
| swagger-ui/swagger-ui-bundle.js | swagger-ui | Apache-2.0 | `050bc415ee7048dcd881682678f720264e7da5e373f7461d7c58c755305255f7` |
| swagger-ui/swagger-ui-bundle.js.LICENSE.txt | swagger-ui | Apache-2.0 | `63818894e4b04cd0e3180d9cb20761e227a939121e7484f8e1d528227c756f89` |
| swagger-ui/swagger-ui.css | swagger-ui | Apache-2.0 | `1ac324f7dcd27e4b9386b4bd6421271ec147e922a22c05ba24b11515e9aa6321` |

## Verify

Recompute any row with `sha256sum <file>` from this directory and compare. Update this file whenever a library is upgraded, together with the licence file next to it.
