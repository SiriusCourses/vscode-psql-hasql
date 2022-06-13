import * as vscode from 'vscode';
import { Pool, DatabaseError } from 'pg';

type TypeCasts = {
  [relativePath: string]: {
    [cyrb53: number]: {
      [positionalParameter: number]: string
    }
  }
};

type DefaultValues = {
  [relativePath: string]: {
    [cyrb53: number]: {
      [positionalParameter: number]: unknown
    }
  }
};

let pgPool: Pool | null = null;

// See https://stackoverflow.com/a/52171480
const cyrb53 = function(str: string, seed = 0): number {
  let h1 = 0xdeadbeef ^ seed, h2 = 0x41c6ce57 ^ seed;
  for (let i = 0, ch; i < str.length; i++) {
      ch = str.charCodeAt(i);
      h1 = Math.imul(h1 ^ ch, 2654435761);
      h2 = Math.imul(h2 ^ ch, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1>>>16), 2246822507) ^ Math.imul(h2 ^ (h2>>>13), 3266489909);
  h2 = Math.imul(h2 ^ (h2>>>16), 2246822507) ^ Math.imul(h1 ^ (h1>>>13), 3266489909);
  return 4294967296 * (2097151 & h2) + (h1>>>0);
};

const createPool = async (
  configuration: vscode.WorkspaceConfiguration,
  log: vscode.OutputChannel
): Promise<Pool> => {
  try {
    const pool = new Pool({
      host: configuration.get('dbHost'),
      port: configuration.get('dbPort'),
      user: configuration.get('dbUser'),
      password: configuration.get('dbUser'),
      database: configuration.get('dbName')
    });

    await pool.query("SELECT 1");

    if (pgPool) {
      await pgPool.end();
    }    
    pgPool = pool;

    log.appendLine(`Connection is established!`);

    return pool;
  } catch (e) {
    log.appendLine(`Failed to test database connection: ${(e as Error).message}`);
    throw e;
  }
};

function getSubstitutionsForDocument<T extends (TypeCasts | DefaultValues)>(
  document: vscode.TextDocument,
  casts: T
): T[string] {
  const key = vscode.workspace.asRelativePath(document.fileName);
  return Object.assign({}, casts[key] ?? {}) as any;
}

export function activate(context: vscode.ExtensionContext) {
  const diagnostics = vscode.languages.createDiagnosticCollection('psql_hasql');
  context.subscriptions.push(diagnostics);

  const log = vscode.window.createOutputChannel('PSQL-Hasql');
  log.appendLine('PSQL-Hasql activated');

  const configuration = vscode.workspace.getConfiguration('psql_hasql');

  let 
    typeCasts: TypeCasts = configuration.get('typeCasts') || {},
    defaultValues: TypeCasts = configuration.get('defaultValues') || {};

  createPool(configuration, log).then((pool) => {
    context.subscriptions.push(
      vscode.workspace.onDidChangeConfiguration((e) => {
        if (
            !e.affectsConfiguration('psql_hasql.typeCasts')
            &&
            !e.affectsConfiguration('psql_hasql.defaultValues')
            ) { return; }  
        const updatedConfiguration = vscode.workspace.getConfiguration('psql_hasql');
        typeCasts = updatedConfiguration.get('typeCasts') ?? {};
        defaultValues = updatedConfiguration.get('defaultValues') ?? {};

        diagnostics.forEach((uri) => {
          const fn = async () => {
            const 
              document = await vscode.workspace.openTextDocument(uri);

            return runValidation(
              getSubstitutionsForDocument(document, typeCasts),
              getSubstitutionsForDocument(document, defaultValues), 
              document, log, pool, diagnostics
            );
          };

          fn().catch(console.error);
        });
      })
    );

    context.subscriptions.push(
      vscode.workspace.onDidOpenTextDocument((document) => {
        if (document.languageId === 'haskell') {
          runValidation(
            getSubstitutionsForDocument(document, typeCasts),
            getSubstitutionsForDocument(document, defaultValues), 
            document, log, pool, diagnostics
          ).catch(console.error);
        }
      })
    );
  
    context.subscriptions.push(
      vscode.workspace.onDidSaveTextDocument((document) => {
        runValidation(
          getSubstitutionsForDocument(document, typeCasts),
          getSubstitutionsForDocument(document, defaultValues), 
          document, log, pool, diagnostics
        ).catch(console.error);
      })
    );
  
    context.subscriptions.push(
      vscode.workspace.onDidCloseTextDocument((doc) => {
        diagnostics.delete(doc.uri);
      })
    );  

    const activeEditor = vscode.window.activeTextEditor;
    if (activeEditor) { 
      const
        curPos = activeEditor.selection.active,
        document = activeEditor.document,
        offset = document.offsetAt(curPos);

      runValidation(
        getSubstitutionsForDocument(document, typeCasts),
        getSubstitutionsForDocument(document, defaultValues), 
        document, log, pool, diagnostics
      ).catch(console.error);
    }  
  }).catch(console.error);

  const virtualDocumentContents = new Map<string, string>();
  const virtualDocumentContentsDidChange = new vscode.EventEmitter<vscode.Uri>();

  vscode.workspace.registerTextDocumentContentProvider('embedded-content', {
    onDidChange: virtualDocumentContentsDidChange.event,
    provideTextDocumentContent: uri => {
      // const originalUri = uri.path.slice(1).slice(0, -4);
      // const decodedUri = decodeURIComponent(originalUri);
      return virtualDocumentContents.get(uri.toString());
    }
  });

  // const activeEditor = vscode.window.activeTextEditor;
  // if (activeEditor) { 
  //   const
  //     curPos = activeEditor.selection.active,
  //     offset = activeEditor.document.offsetAt(curPos);

  //   runCheck(activeEditor.document, virtualDocumentContents, virtualDocumentContentsDidChange)
  //   .map((uri) => {
  //     const fn = async () => {
  //       const document = await vscode.workspace.openTextDocument(uri);
  //       await vscode.languages.setTextDocumentLanguage(document, "postgres");
  //       console.log(document);
  //     };

  //     fn().catch(console.error);
  //   }); 
  // }
}

const HASKELL_HASQL = 'hasql\|';
const HASKELL_HASQL_END = '\|\]';

// Copied from https://github.com/barklan/inline_sql_syntax/blob/main/src/extension.ts
function getHasqlContent(
  doc: vscode.TextDocument
): readonly [ReadonlyArray<vscode.Range>, boolean] {
  const result: vscode.Range[] = [];

  let startRangePosition = -1;
  let sqlStringBound = null;
  let sqlStartLineIndex = -1;

  let hasqlPatternStart = -1;
  for (let lineIndex = 0; lineIndex < doc.lineCount; lineIndex += 1) {
    const lineOfText = doc.lineAt(lineIndex).text;
    if (sqlStartLineIndex === -1) {
      if ((hasqlPatternStart = lineOfText.indexOf(HASKELL_HASQL)) !== -1) {
        startRangePosition = hasqlPatternStart + HASKELL_HASQL.length;
        sqlStringBound = HASKELL_HASQL_END;
        sqlStartLineIndex = lineIndex;
      }
    }
    if (sqlStringBound && sqlStringBound !== '') {
      const endSqlIndex = lineOfText.indexOf(sqlStringBound);
      if (endSqlIndex !== -1) {
        const range = new vscode.Range(
          sqlStartLineIndex,
          startRangePosition,
          lineIndex,
          endSqlIndex,
        );
        result.push(range);
        sqlStartLineIndex = -1;
        sqlStringBound = '';
      }
    }
  }

  if (sqlStringBound !== '') {
    return [result, false] as const;
  }

  return [result, true] as const;
}

const normalizeEnding = (v: string): string => {
  const 
    trimmed = v.trimEnd(),
    isEnded = trimmed[trimmed.length-1] === ';';

  return trimmed + (isEnded ? '' : ';');
};

const wrapExpressionInSyntaxCheck = (v: string): readonly [string, vscode.Position] => {
  const 
    isMultiLine = v.includes('\n'),
    normalized = normalizeEnding(v);

  return [`DO $SYNTAX_CHECK$ BEGIN RETURN;
${normalized}
END; $SYNTAX_CHECK$;
`.trim(), new vscode.Position(1 + (isMultiLine ? 1 : 0), 0)] as const;
};

const wrapExpressionInExplainCheck = (v: string): readonly [string, vscode.Position] => {
  const 
    isMultiLine = v.includes('\n'),
    normalized = normalizeEnding(v);

  return [`
EXPLAIN
${normalized}
`.trim(), new vscode.Position(1 + (isMultiLine ? 1 : 0), 0)] as const;
};

async function runSyntaxCheck(
  expression: string,
  expressionHash: number,
  range: vscode.Range,
  log: vscode.OutputChannel,
  pool: Pool
): Promise<DatabaseError | null> {
  const 
    [testExpression, { line }] = wrapExpressionInSyntaxCheck(expression),
    hostLine = range.start.line + line;
  
  log.appendLine(`Validation script for line ${hostLine} expression (${expressionHash}) is \n---\n${testExpression}\n===\n`);

  try {
    await pool.query(testExpression);

    return null;
  } catch (e: unknown) {
    return e as DatabaseError;
  }
}

async function runExplainCheck(
  predefinedTypes: TypeCasts[string][number],
  predefinedValues: DefaultValues[string][number],
  expression: string,
  expressionHash: number,
  range: vscode.Range,
  log: vscode.OutputChannel,
  pool: Pool
): Promise<DatabaseError | null> {
  const 
    [testExpression, { line }] = wrapExpressionInExplainCheck(expression),
    hostLine = range.start.line + line,
    parameters = (new Set([...expression.matchAll(/(\$[1-9][0-9]*)/g)].map((found) => found[0]))).size,
    parameterValues = Array(parameters)
      .fill(null)
      .map((v, i) => {
        if (predefinedValues[i+1]) { return predefinedValues[i+1]; }
        return v;
      }),
    knownTypes = Object.keys(predefinedTypes),
    actualQuery = knownTypes.reduce((acc, v: string) => {
      const position = parseInt(v);
      return acc.replace(new RegExp(`\\$${position}`, "gm"), `$${position}::${predefinedTypes[position]}`);
    }, testExpression);

  if (knownTypes.length > 0) {
    log.appendLine(`Script line ${hostLine} has types substitutions, its origin query will be changed`);
  }

  if (Object.keys(predefinedValues).length > 0) {
    log.appendLine(`Script line ${hostLine} has default values, "null" value will be replaced with provided ones: ${JSON.stringify(parameterValues)}`);
  }

  log.appendLine(`Validation script for line ${hostLine} expression (${expressionHash}) is \n---\n${actualQuery}\n===\n`);

  try {
    await pool.query(actualQuery, parameterValues);
    
    return null;
  } catch (e: unknown) {
    return e as DatabaseError;
  }
}

async function runValidation(
  typeCasts: TypeCasts[string],
  defaultValues: DefaultValues[string],
  document: vscode.TextDocument,
  log: vscode.OutputChannel,
  pool: Pool,
  diagnostics: vscode.DiagnosticCollection
): Promise<void> {
  const hasqls = getHasqlContent(document);

  let diagnosticsCurrent: vscode.Diagnostic[] = [];

  if (!hasqls[1]) { 

    log.appendLine(`Failed to parse the document: some expressions seemed to be disclosed.`);
    return;
  }

  log.appendLine(`Detected ${hasqls[0].length} psql expressions at the document: ${document.uri.toString()}!`);

  const result = await Promise.all(
    hasqls[0].map(async (h, i) => {
      const 
        expression = document.getText(h),
        expressionEssential = expression.replace(/(\r\n|\n|\r)/gm, "").replace(/\s/g, '').trim().toLocaleLowerCase(),
        expressionHash = cyrb53(expressionEssential),
        expressionTypeCasts = typeCasts[expressionHash] ?? {},
        expressionDefaultValues = defaultValues[expressionHash] ?? {},
        syntaxError = await runSyntaxCheck(expression, expressionHash, h, log, pool),
        explainError = await runExplainCheck(expressionTypeCasts, expressionDefaultValues, expression, expressionHash, h, log, pool);

        log.appendLine(`Hash ${expressionHash} generated for query at line ${h.start.line}: ${expressionEssential}!`);

      if (!syntaxError && !explainError) {
        return true;
      }

      // Only one error might happen at one time
      const actualError = (syntaxError || explainError)!; 

      diagnosticsCurrent.push(new vscode.Diagnostic(h, [
        `Query cyrb53 hash: ${expressionHash}`,
        actualError.message,
        actualError.hint
      ].filter((v) => !!v).join('\n')));

      diagnostics.set(document.uri, diagnosticsCurrent);
      return false;
    })
  );

  diagnostics.set(document.uri, diagnosticsCurrent);
  
  const correctAmount = result.filter((v) => !!v).length;

  log.appendLine(`Expressions correct ${correctAmount}/${result.length}!`);
}

function runCheck(
  document: vscode.TextDocument,
  virtualDocumentContents: Map<string, string>,
  virtualDocumentContentsDidChange: vscode.EventEmitter<vscode.Uri>
): ReadonlyArray<vscode.Uri> {
  const hasqls = getHasqlContent(document);

  const originalUri = document.uri.toString();

  if (hasqls[1]) {
    return hasqls[0].map((h, i) => {
      const vdocUriString = `embedded-content://postgres/${encodeURIComponent(originalUri).replace(/\./g, '_dot_')}-${i}.pgsql`;
      const vdocUri = vscode.Uri.parse(vdocUriString);

      console.log(originalUri, vdocUriString);

      virtualDocumentContents.set(vdocUri.toString(), document.getText(h));
      virtualDocumentContentsDidChange.fire(vdocUri);

      console.log(i, document.getText(h));

      vscode.commands.executeCommand<vscode.CompletionList>(
        'vscode.executeCompletionItemProvider',
        vdocUri,
        new vscode.Position(0, 0),
        'k'
      );

      return vdocUri;
    });
  }

  return [];
}

// this method is called when your extension is deactivated
export async function deactivate(): Promise<void> {
  if (pgPool) {
    await pgPool.end();
  }
}
