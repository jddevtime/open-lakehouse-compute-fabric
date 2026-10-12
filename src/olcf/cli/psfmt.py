# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""PowerShell tab completion for the ``olcf`` CLI (click ships bash, zsh and fish only).

The generated script registers a native argument completer that re-invokes ``olcf`` with ``_OLCF_COMPLETE=powershell_complete``; the
completion class below answers with one ``value<TAB>help`` line per candidate.  Words are passed joined by ``U+001F`` so empty words survive.
"""

from __future__ import annotations

import os
from typing import Any

from click.shell_completion import CompletionItem, ShellComplete, add_completion_class

SOURCE = r"""Register-ArgumentCompleter -Native -CommandName %(prog_name)s -ScriptBlock {
    param($wordToComplete, $commandAst, $cursorPosition)
    $words = @($commandAst.CommandElements | ForEach-Object { $_.ToString() })
    if ($wordToComplete -eq '') { $words += '' }
    $env:COMP_WORDS = ($words -join [char]31)
    $env:COMP_CWORD = [string]($words.Count - 1)
    $env:%(complete_var)s = 'powershell_complete'
    try {
        & %(prog_name)s 2>$null | ForEach-Object {
            $parts = $_ -split "`t", 2
            $help = if ($parts.Count -gt 1 -and $parts[1]) { $parts[1] } else { $parts[0] }
            [System.Management.Automation.CompletionResult]::new($parts[0], $parts[0], 'ParameterValue', $help)
        }
    } finally {
        Remove-Item Env:\COMP_WORDS, Env:\COMP_CWORD, Env:\%(complete_var)s -ErrorAction SilentlyContinue
    }
}
"""


class PowerShellComplete(ShellComplete):
    """Click shell-completion support for PowerShell, which click does not provide itself."""
    name = "powershell"
    source_template = SOURCE

    def get_completion_args(self) -> tuple[list[str], str]:
        words = os.environ.get("COMP_WORDS", "").split("\x1f")
        cword = int(os.environ.get("COMP_CWORD", str(len(words) - 1)))
        args = words[1:cword]
        incomplete = words[cword] if cword < len(words) else ""
        return args, incomplete

    def format_completion(self, item: CompletionItem) -> str:
        return f"{item.value}\t{item.help or ''}"


def register_powershell() -> None:
    add_completion_class(PowerShellComplete, "powershell")


register_powershell()
__all__: list[Any] = ["PowerShellComplete", "register_powershell"]
