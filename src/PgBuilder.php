<?php

class PgBuilder extends Xplend
{
    public $queries = array();
    public $queries_mini = array();
    public $queries_color = array();
    public $mute = false;
    public $create_database = false;
    public $create_database_count = 0;
    public $select_database = '';
    public $select_tenant = '';
    private $actions = 0;

    private $postgresTypeDictionary = [
        'SERIAL' => 'integer',
        'VARCHAR' => 'character varying',
        'INT' => 'integer',
        'INTEGER' => 'integer',
        'TEXT' => 'text',
        'TIMESTAMP' => 'timestamp without time zone',
        'DATE' => 'date',
        'TIME' => 'time without time zone',
        'BOOLEAN' => 'boolean',
        'SMALLINT' => 'smallint',
        'BIGINT' => 'bigint',
        'REAL' => 'real',
        'DOUBLE' => 'double precision',
        'NUMERIC' => 'numeric',
        'DECIMAL' => 'numeric',
        'JSON' => 'json',
        'JSONB' => 'jsonb',
        'UUID' => 'uuid',
    ];

    // Custom fields
    public $custom_fields = [];

    private static $instance = null;

    public function __construct()
    {
        global $_APP;
        if (!empty($_APP['POSTGRES']['CUSTOM_FIELDS'])) {
            $this->custom_fields = @$_APP['POSTGRES']['CUSTOM_FIELDS'];
        }
        if (!is_writable(self::DIR_SCHEMA)) {
            // die('ERROR:' . realpath(self::DIR_SCHEMA) . ' is not writable.' . PHP_EOL);
        }
    }

    public static function getInstance()
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    public static function getParams()
    {
        return self::getInstance()->custom_fields;
    }

    // ----------------------------
    // Helpers de output/queue
    // ----------------------------

    private function headerTable($table)
    {
        if ($this->mute)
            return;
        Mason::header("∴ $table", 'blue');
    }

    private function sayUpToDate($table)
    {
        if ($this->mute)
            return;
        // Mensagem curta, abaixo do cabeçalho
        Mason::say("✓ Table is up to date");
    }

    private function pushQuery($sql, $mini = null, $color = 'green')
    {
        $sql = removeExtraSpaces($sql);

        $this->queries[] = $sql;

        // Resumo final SEMPRE mini. Se não vier mini, gera um fallback.
        if ($mini === null || trim($mini) === '') {
            $mini = $this->miniFromSql($sql);
        }

        $this->queries_mini[] = $mini;
        $this->queries_color[] = $color;

        $this->actions++;

        if (!$this->mute) {
            Mason::say("→ $sql", $color);
        }
    }

    // Fallback simples: nunca deixa o resumo cair no SQL completo
    private function miniFromSql($sql)
    {
        $s = trim(preg_replace('/\s+/', ' ', (string) $sql));

        if (stripos($s, 'CREATE TABLE') === 0) {
            if (preg_match('/CREATE TABLE\s+"([^"]+)"/i', $s, $m))
                return "CREATE TABLE \"{$m[1]}\" ...";
            return "CREATE TABLE ...";
        }

        if (stripos($s, 'ALTER TABLE') === 0) {
            if (preg_match('/ALTER TABLE\s+"([^"]+)"/i', $s, $m))
                return "ALTER TABLE \"{$m[1]}\" ...";
            return "ALTER TABLE ...";
        }

        if (stripos($s, 'DROP TABLE') === 0) {
            if (preg_match('/DROP TABLE IF EXISTS\s+"([^"]+)"/i', $s, $m))
                return "DROP TABLE \"{$m[1]}\" ...";
            return "DROP TABLE ...";
        }

        if (stripos($s, 'CREATE INDEX') === 0) {
            if (preg_match('/CREATE INDEX\s+"([^"]+)"/i', $s, $m))
                return "ADD INDEX \"{$m[1]}\" ...";
            return "ADD INDEX ...";
        }

        if (stripos($s, 'DROP INDEX') === 0) {
            if (preg_match('/DROP INDEX IF EXISTS\s+"([^"]+)"/i', $s, $m))
                return "DROP INDEX \"{$m[1]}\" ...";
            return "DROP INDEX ...";
        }

        // Genérico (limitado)
        return substr($s, 0, 80) . (strlen($s) > 80 ? " ..." : "");
    }

    // ----------------------------
    // DEFAULT: YAML -> SQL
    // ----------------------------

    // Converte o "default/" do yaml para SQL sem quebrar compatibilidade.
    // Regras:
    // - números e boolean: sem aspas
    // - strings: com aspas simples
    // - funções/expressões SQL: sem aspas se terminar com ")" OU for keyword (CURRENT_TIMESTAMP/DATE/TIME)
    // - json/jsonb: se vier {} ou [] => faz cast ::jsonb/::json conforme tipo
    private function normalizeDefaultSql($rawDefault, $typeRealUpper)
    {
        if ($rawDefault === null)
            return null;

        $raw = trim((string) $rawDefault);
        if ($raw === '')
            return null;

        // Se vier "null" explícito, tratamos como "sem default" (não emite DEFAULT NULL).
        if (strtolower($raw) === 'null')
            return null;

        // Se já vier com "DEFAULT ..." (usuário avançado), aceita e remove o prefixo
        if (stripos($raw, 'default ') === 0) {
            $raw = trim(substr($raw, 7));
        }

        // Funções/expressões SQL simples (termina com ")") ou keywords SQL
        $upperRaw = strtoupper($raw);
        if (preg_match('/\)\s*$/', $raw) || in_array($upperRaw, ['CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME'])) {
            return $raw;
        }

        // Boolean
        if (in_array(strtolower($raw), ['true', 'false'], true)) {
            return strtoupper($raw);
        }

        // Numérico (int/float)
        if (preg_match('/^-?\d+(\.\d+)?$/', $raw)) {
            return $raw;
        }

        // JSON / JSONB
        if (strpos($typeRealUpper, 'JSONB') !== false || strpos($typeRealUpper, 'JSON') !== false) {
            // Se vier objeto/array, coloca aspas e faz cast.
            $first = substr($raw, 0, 1);
            if ($first === '{' || $first === '[') {
                $escaped = str_replace("'", "''", $raw);
                if (strpos($typeRealUpper, 'JSONB') !== false)
                    return "'$escaped'::jsonb";
                return "'$escaped'::json";
            }
            // Se já veio algo avançado (ex: '...'::jsonb), deixa passar
            return $raw;
        }

        // UUID literal
        if (preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i', $raw)) {
            return "'" . strtolower($raw) . "'";
        }

        // Se já estiver entre aspas simples, respeita
        if (preg_match("/^'(.*)'$/s", $raw)) {
            return $raw;
        }

        // Se estiver entre aspas duplas, converte para aspas simples
        if (preg_match('/^"(.*)"$/s', $raw, $m)) {
            $raw = $m[1];
        }

        // Default string
        $escaped = str_replace("'", "''", $raw);
        return "'$escaped'";
    }

    private function buildDefaultClause($rawDefault, $typeRealUpper)
    {
        $sql = $this->normalizeDefaultSql($rawDefault, $typeRealUpper);
        if ($sql === null)
            return '';
        return "DEFAULT $sql";
    }

    // Normaliza column_default do Postgres só pra comparação (evita falso mismatch)
    // Exemplos:
    // - "'0'::integer" -> "0"
    // - "0" -> "0"
    // - "true::boolean" -> "true"
    // - "now()" -> "now()"
    // - "nextval('tbl_id_seq'::regclass)" -> "nextval('tbl_id_seq'::regclass)" (não mexe)
    private function normalizeDbDefaultForCompare($dbDefault)
    {
        $d = trim((string) $dbDefault);
        if ($d === '')
            return '';

        $d = preg_replace('/\s+/', ' ', $d);

        // não tenta normalizar nextval, pq é serial/sequence
        if (stripos($d, 'nextval(') !== false)
            return $d;

        // remove casts finais ::tipo (um ou mais)
        // ex: "'0'::integer" / "true::boolean"
        while (preg_match('/^(.*)::[a-zA-Z0-9_ ]+$/', $d, $m)) {
            $d = trim($m[1]);
        }

        // remove parênteses externos simples
        if (preg_match('/^\((.*)\)$/', $d, $m)) {
            $d = trim($m[1]);
        }

        // remove aspas simples externas (se virar literal)
        if (preg_match("/^'(.*)'$/s", $d, $m)) {
            $d = $m[1];
        }

        // boolean pra minúsculo na comparação
        if (in_array(strtolower($d), ['true', 'false'], true)) {
            $d = strtolower($d);
        }

        // números ok
        if (preg_match('/^-?\d+(\.\d+)?$/', $d)) {
            return $d;
        }

        return $d;
    }

    private function convertField($field)
    {
        $new_field = array();
        $individual_indexes = [];
        $composite_indexes = [];
        $composite_unique_indexes = [];

        if (!is_array($field))
            goto convertFieldEnd;

        foreach ($field as $k => $v) {
            $parts = explode(" ", $v);

            // find field type
            $type_part = $parts[0];
            $type = explode("/", $type_part)[0];
            $type_from_custom = @$this->custom_fields[$type]['Type'];
            $type_real = '';
            if ($type_from_custom)
                $type_real = $type_from_custom;
            else {
                $type_real = $type;
                $this->custom_fields[$type_real] = [
                    'Type' => '',
                    'Null' => '',
                    'Default' => '',
                    'Extra' => ''
                ];
            }

            // Field lenght limit
            $len = @explode("/", $type_part)[1];
            if ($len) {
                $type_real = preg_replace('/\(\d+\)/', '', $type_real);
                $type_real = "$type_real($len)";
            }

            // Define default vindo do yaml: default/xxx
            $default_raw = null;
            foreach ($parts as $part) {
                if (strpos($part, 'default/') === 0) {
                    $default_raw = substr($part, strlen('default/'));
                    break;
                }
            }

            // Fallback: default vindo de custom_fields (mantém compatibilidade)
            if ($default_raw === null) {
                $default_from_custom = @$this->custom_fields[$type]['Default'];
                if ($default_from_custom !== '' && $default_from_custom !== null) {
                    $default_raw = $default_from_custom;
                }
            }

            // Define null or not null
            if ($type === 'id' || strpos($type_real, 'SERIAL') !== false) {
                $null = '';
            } else {
                $req = array_search('required', $parts);
                $null = ($req !== false) ? "NOT NULL" : "NULL";
            }

            // Define key
            $key = '';
            $key_from_custom = @$this->custom_fields[$type]['Key'];
            // unique simples (sem /) = constraint individual
            if (array_search('unique', $parts) !== false)
                $key = 'UNI';
            if ($key_from_custom)
                $key = $key_from_custom;

            // Define indexes and composite unique indexes
            foreach ($parts as $part) {
                // index ou index/nome
                if (strpos($part, 'index') === 0) {
                    $index_parts = explode("/", $part);
                    if (isset($index_parts[1])) {
                        $index_names = explode(",", $index_parts[1]);
                        foreach ($index_names as $index_name) {
                            if (!isset($composite_indexes[$index_name])) {
                                $composite_indexes[$index_name] = [];
                            }
                            $composite_indexes[$index_name][] = $k;
                        }
                    } else {
                        $individual_indexes[] = $k;
                    }
                }
                // unique/nome = composite unique index
                if (strpos($part, 'unique/') === 0) {
                    $unique_parts = explode("/", $part);
                    if (isset($unique_parts[1])) {
                        $unique_names = explode(",", $unique_parts[1]);
                        foreach ($unique_names as $unique_name) {
                            if (!isset($composite_unique_indexes[$unique_name])) {
                                $composite_unique_indexes[$unique_name] = [];
                            }
                            $composite_unique_indexes[$unique_name][] = $k;
                        }
                    }
                }
            }

            $new_field[$k] = array(
                'Field' => $k,
                'Type' => strtoupper($type_real),
                'Null' => $null,
                'Key' => $key,
                'Default' => $default_raw,
                'Extra' => @strtoupper(@$this->custom_fields[$type]['Extra']),
            );
        }

        convertFieldEnd:
        return [
            'fields' => $new_field,
            'individual_indexes' => array_unique($individual_indexes),
            'composite_indexes' => $composite_indexes,
            'composite_unique_indexes' => $composite_unique_indexes
        ];
    }

    private function createTable($table, $schema, $pg)
    {
        $this->headerTable($table);
        $actions_before = $this->actions;

        $_comma = '';
        $query = "CREATE TABLE \"$table\" (" . PHP_EOL;

        $unique_fields = [];
        $index_fields = [];
        $composite_indexes = $schema['composite_indexes'];
        $individual_indexes = $schema['individual_indexes'];
        $fields = $schema['fields'];

        foreach ($fields as $k => $v) {
            $type = strtoupper($v['Type']);
            $null = ($v['Null'] === 'NOT NULL') ? "NOT NULL" : ($v['Null'] === '' ? '' : "");
            $extra = strtoupper(@$v['Extra']);

            // SERIAL já cria default nextval automaticamente no Postgres, não força DEFAULT aqui.
            $default = '';
            if (strpos($type, 'SERIAL') === false) {
                $default = $this->buildDefaultClause(@$v['Default'], $type);
            }

            $query .= $_comma . "\"$k\" $type $null $default $extra";

            if (@$v['Key'] === 'PRI') {
                $query .= " PRIMARY KEY";
            }

            if (@$v['Key'] === 'UNI') {
                $unique_fields[] = $k;
            }

            if (in_array($k, $individual_indexes)) {
                $index_fields[] = $k;
            }

            $_comma = ', ' . PHP_EOL;
        }

        $query .= PHP_EOL . ");";

        $this->pushQuery($query, "CREATE TABLE \"$table\" ...", 'green');

        foreach ($unique_fields as $unique_field) {
            $q = "ALTER TABLE \"$table\" ADD CONSTRAINT \"{$table}_{$unique_field}_unique\" UNIQUE (\"$unique_field\");";
            $this->pushQuery($q, "ADD UNIQUE \"{$table}_{$unique_field}_unique\" ...", 'cyan');
        }

        foreach ($index_fields as $index_field) {
            $q = "CREATE INDEX CONCURRENTLY \"{$table}_{$index_field}_idx\" ON \"$table\" (\"$index_field\");";
            $this->pushQuery($q, "ADD INDEX \"{$table}_{$index_field}_idx\" ...", 'cyan');
        }

        foreach ($composite_indexes as $index_name => $columns) {
            $columns_str = implode('", "', $columns);
            $q = "CREATE INDEX CONCURRENTLY \"{$table}_{$index_name}_idx\" ON \"$table\" (\"$columns_str\");";
            $this->pushQuery($q, "ADD INDEX \"{$table}_{$index_name}_idx\" ...", 'cyan');
        }


        // Composite unique indexes (unique/nome_grupo)
        $composite_unique_indexes = $schema['composite_unique_indexes'] ?? [];
        foreach ($composite_unique_indexes as $index_name => $columns) {
            $columns_str = implode('", "', $columns);
            $q = "CREATE UNIQUE INDEX CONCURRENTLY \"{$table}_{$index_name}_unique_idx\" ON \"$table\" (\"$columns_str\");";
            $this->pushQuery($q, "ADD UNIQUE INDEX \"{$table}_{$index_name}_unique_idx\" ...", 'cyan');
        }

        // Se algum dia você decidir “pular create” por alguma regra, evita ficar mudo.
        if ($this->actions === $actions_before) {
            $this->sayUpToDate($table);
        }
    }

    private function updateTable($table, $schema, $field_curr, $pg)
    {
        $this->headerTable($table);
        $actions_before = $this->actions;

        $fields = $schema['fields'];
        $individual_indexes = $schema['individual_indexes'];
        $composite_indexes = $schema['composite_indexes'];
        $composite_unique_indexes = $schema['composite_unique_indexes'] ?? [];

        // Fetch existing indexes
        $existing_indexes = $pg->query("
            SELECT indexname 
            FROM pg_indexes 
            WHERE tablename = '$table'
        ");

        // Fetch existing UNIQUE constraints
        $existing_uniques = $pg->query("
            SELECT conname 
            FROM pg_constraint 
            WHERE conrelid = '$table'::regclass 
            AND contype = 'u'
        ");

        $existing_index_names = [];
        foreach ($existing_indexes as $index) {
            $existing_index_names[] = $index['indexname'];
        }

        $existing_unique_names = [];
        foreach ($existing_uniques as $unique) {
            $existing_unique_names[] = $unique['conname'];
        }

        // Expected indexes and UNIQUE constraints from configuration
        $expected_indexes = [];
        $expected_unique_names = [];

        foreach ($individual_indexes as $index_field) {
            $expected_indexes[] = "{$table}_{$index_field}_idx";
        }

        foreach ($composite_indexes as $index_name => $columns) {
            $expected_indexes[] = "{$table}_{$index_name}_idx";
        }

        // Composite unique indexes (unique/nome_grupo)
        foreach ($composite_unique_indexes as $index_name => $columns) {
            $expected_indexes[] = "{$table}_{$index_name}_unique_idx";
        }

        foreach ($fields as $k => $v) {
            if (@$v['Key'] === 'UNI') {
                $expected_unique_names[] = "{$table}_{$k}_unique";
                $expected_indexes[] = "{$table}_{$k}_unique";
            }
            if (@$v['Key'] === 'PRI') {
                $expected_indexes[] = "{$table}_pkey";
            }
        }

        // Drop columns not in new configuration
        foreach ($field_curr as $column => $data) {
            if (!isset($fields[$column])) {
                $q = "ALTER TABLE \"$table\" DROP COLUMN \"$column\";";
                $this->pushQuery($q, "DROP COLUMN \"$table\".\"$column\" ...", 'yellow');
            }
        }

        // Remove UNIQUE constraints not in configuration
        foreach ($existing_unique_names as $unique_name) {
            if (!in_array($unique_name, $expected_unique_names)) {
                $q = "ALTER TABLE \"$table\" DROP CONSTRAINT \"$unique_name\";";
                $this->pushQuery($q, "DROP CONSTRAINT \"$unique_name\" ...", 'yellow');
            }
        }

        // Remove indexes not in configuration
        foreach ($existing_index_names as $index_name) {
            if (!in_array($index_name, $expected_indexes)) {
                $q = "DROP INDEX IF EXISTS \"$index_name\";";
                $this->pushQuery($q, "DROP INDEX \"$index_name\" ...", 'yellow');
            }
        }

        // Add new columns
        foreach ($fields as $k => $v) {
            if (!isset($field_curr[$k])) {
                $typeUpper = strtoupper($v['Type']);

                // SERIAL já cria default nextval automaticamente no Postgres, não força DEFAULT aqui.
                $default = '';
                if (strpos($typeUpper, 'SERIAL') === false) {
                    $default = $this->buildDefaultClause(@$v['Default'], $typeUpper);
                }

                $q = "ALTER TABLE \"$table\" ADD COLUMN \"$k\" " . $typeUpper . " " . $v['Null'] . " $default " . $v['Extra'] . ";";
                $this->pushQuery($q, "ADD COLUMN \"$table\".\"$k\" ...", 'cyan');
            }
        }

        // Update existing columns if field type or length differs
        foreach ($fields as $k => $v) {
            if (!isset($field_curr[$k]))
                continue;

            // Extract configured base type and length (if provided)
            if (preg_match('/^(\w+)(?:\((\d+)\))?$/', $v['Type'], $matches)) {
                $configBaseType = strtoupper($matches[1]);
                $configLength = isset($matches[2]) ? (int) $matches[2] : null;
            } else {
                $configBaseType = @explode("(", strtoupper($v['Type']))[0];
                $configLength = null;
            }

            // Map the config base type using the dictionary
            if (isset($this->postgresTypeDictionary[$configBaseType])) {
                $mappedConfigType = $this->postgresTypeDictionary[$configBaseType];
            } else {
                $mappedConfigType = strtolower($configBaseType);
            }

            // Get the current database field type and length
            $dbType = strtolower($field_curr[$k]['data_type']);
            $dbLength = isset($field_curr[$k]['character_maximum_length']) ? (int) $field_curr[$k]['character_maximum_length'] : null;

            // If the base type is different, update with the new type and length (if provided)
            if ($mappedConfigType !== $dbType) {
                $q = "ALTER TABLE \"$table\" ALTER COLUMN \"$k\" TYPE " . strtoupper($v['Type']) . ";";
                $this->pushQuery($q, "ALTER TYPE \"$table\".\"$k\" -> " . strtoupper($v['Type']) . " ...", 'cyan');
            }
            // If the type is the same but the length differs, update the column type with the new length
            else if ($configLength !== null && $dbLength !== $configLength) {
                $q = "ALTER TABLE \"$table\" ALTER COLUMN \"$k\" TYPE " . strtoupper($v['Type']) . ";";
                $this->pushQuery($q, "ALTER TYPE \"$table\".\"$k\" -> " . strtoupper($v['Type']) . " ...", 'cyan');
            }
        }

        // Atualiza DEFAULT (SET/DROP)
        foreach ($fields as $k => $v) {
            if (!isset($field_curr[$k]))
                continue;

            $typeUpper = strtoupper($v['Type']);
            $dbDefaultRaw = isset($field_curr[$k]['column_default']) ? (string) $field_curr[$k]['column_default'] : '';

            // 1) NUNCA mexer em default de SERIAL (id custom field) => evita DROP do nextval()
            if (strpos($typeUpper, 'SERIAL') !== false) {
                continue;
            }

            // 2) Se for PRI e o banco tem nextval(...), também não mexe (cobre casos de integer + sequence legado)
            if (@$v['Key'] === 'PRI' && stripos($dbDefaultRaw, 'nextval(') !== false) {
                continue;
            }

            $configDefaultClause = $this->buildDefaultClause(@$v['Default'], $typeUpper);
            $configDefaultSql = '';
            if ($configDefaultClause) {
                $configDefaultSql = trim(substr($configDefaultClause, strlen('DEFAULT ')));
            }

            $dbDefaultNorm = $this->normalizeDbDefaultForCompare($dbDefaultRaw);
            $cfgDefaultNorm = $this->normalizeDbDefaultForCompare($configDefaultSql);

            // Se no YAML não tem default e no banco tem, remove
            if ($cfgDefaultNorm === '' && $dbDefaultNorm !== '') {
                $q = "ALTER TABLE \"$table\" ALTER COLUMN \"$k\" DROP DEFAULT;";
                $this->pushQuery($q, "DROP DEFAULT \"$table\".\"$k\" ...", 'cyan');
                continue;
            }

            // Se no YAML tem default e no banco é diferente, seta
            if ($cfgDefaultNorm !== '' && $dbDefaultNorm !== $cfgDefaultNorm) {
                $q = "ALTER TABLE \"$table\" ALTER COLUMN \"$k\" SET DEFAULT $configDefaultSql;";
                $this->pushQuery($q, "SET DEFAULT \"$table\".\"$k\" ...", 'cyan');
            }
        }

        // Create individual indexes if not exists
        foreach ($individual_indexes as $index_field) {
            $index_name = "{$table}_{$index_field}_idx";
            if (!in_array($index_name, $existing_index_names)) {
                $q = "CREATE INDEX CONCURRENTLY \"$index_name\" ON \"$table\" (\"$index_field\");";
                $this->pushQuery($q, "ADD INDEX \"$index_name\" ...", 'cyan');
            }
        }

        // Create composite indexes if not exists
        foreach ($composite_indexes as $index_name => $columns) {
            $index_name_full = "{$table}_{$index_name}_idx";
            if (!in_array($index_name_full, $existing_index_names)) {
                $columns_str = implode('", "', $columns);
                $q = "CREATE INDEX CONCURRENTLY \"$index_name_full\" ON \"$table\" (\"$columns_str\");";
                $this->pushQuery($q, "ADD INDEX \"$index_name_full\" ...", 'cyan');
            }
        }

        // Create composite unique indexes if not exists
        foreach ($composite_unique_indexes as $index_name => $columns) {
            $index_name_full = "{$table}_{$index_name}_unique_idx";
            if (!in_array($index_name_full, $existing_index_names)) {
                $columns_str = implode('", "', $columns);
                $q = "CREATE UNIQUE INDEX CONCURRENTLY \"$index_name_full\" ON \"$table\" (\"$columns_str\");";
                $this->pushQuery($q, "ADD UNIQUE INDEX \"$index_name_full\" ...", 'cyan');
            }
        }

        // Create UNIQUE constraints if not exists (unique simples)
        foreach ($fields as $k => $v) {
            if (@$v['Key'] === 'UNI') {
                $unique_name = "{$table}_{$k}_unique";
                if (!in_array($unique_name, $existing_unique_names)) {
                    $q = "ALTER TABLE \"$table\" ADD CONSTRAINT \"$unique_name\" UNIQUE (\"$k\");";
                    $this->pushQuery($q, "ADD UNIQUE \"$unique_name\" ...", 'cyan');
                }
            }
        }

        // Se não houve nenhuma ação para essa tabela, avisa abaixo do cabeçalho
        if ($this->actions === $actions_before) {
            $this->sayUpToDate($table);
        }
    }

    private function deleteTable($table, $pg)
    {
        $this->headerTable($table);

        $q = "DROP TABLE IF EXISTS \"$table\" CASCADE;";
        $this->pushQuery($q, "DROP TABLE \"$table\" ...", 'yellow');
    }

    private function createDatabase($name, $pg)
    {
        $q = "CREATE DATABASE \"$name\" ENCODING 'UTF8';";
        $this->pushQuery($q, "CREATE DATABASE \"$name\" ...", 'green');
        $this->create_database_count++;
    }

    public function buildReverse()
    {
        $table = array();
        $r = jwquery("SELECT table_name FROM information_schema.tables WHERE table_schema='public'");
        for ($i = 0; $i < count($r); $i++) {
            foreach ($r[$i] as $k => $v) {
                $table[] = $v;
            }
        }
        for ($i = 0; $i < count($table); $i++) {
            $field = array();
            $r = jwquery("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{$table[$i]}'");
            for ($x = 0; $x < count($r); $x++) {
                $f_name = $r[$x]['column_name'];
                $f_type = $r[$x]['data_type'];
                $f_null = $r[$x]['is_nullable'];
                // Logic to rebuild structure…
            }
        }
    }

    public function up($argx)
    {
        global $_APP;

        if (@$argx['--mute'])
            $this->mute = true;
        if (@$argx['--create'])
            $this->create_database = true;
        if (@$argx['--name'])
            $this->select_database = $argx['--name'];
        if (@$argx['--tenant'])
            $this->select_tenant = $argx['--tenant'];

        if (!@is_array($_APP['POSTGRES']['DB'])) {
            Mason::say("Ops! config is missing.", "red");
            Mason::say("Please, verify: modules/postgres/config/postgres.yml", "red");
            exit;
        }

        foreach ($_APP['POSTGRES']['DB'] as $db_id => $db_conf) {
            if ($this->select_tenant) {
                if (!@$db_conf['TENANT_KEYS'])
                    continue;
            }

            if ($this->select_database) {
                if ($this->select_database !== $db_conf['NAME'] and !@$db_conf['TENANT_KEYS']) {
                    continue;
                }
            }

            Mason::say("► PostgreSQL '$db_id' ...", 'cyan');

            if (@$db_conf['PATH']) {
                if (!is_array($db_conf['PATH']))
                    $db_conf['PATH'] = [$db_conf['PATH']];
                for ($i = 0; $i < count($db_conf['PATH']); $i++) {
                    $db_conf['PATH'][$i] = realpath(__DIR__ . '/../../../' . $db_conf['PATH'][$i] . '/');
                }
                $databasePaths = $db_conf['PATH'];
            } else {
                $databasePaths = Xplend::findPathsByType("database");
            }

            $pg = new PgService();

            if ($this->create_database) {
                $find_db = $pg->query("SELECT datname FROM pg_database WHERE datname = :name", ['name' => $db_conf['NAME']]);
                if (@!$find_db[0]) {
                    $this->createDatabase($db_conf['NAME'], $pg);
                    goto execute;
                }
            }

            $tables_real = array();
            $t = $pg->query("SELECT table_name FROM information_schema.tables WHERE table_schema='public'");
            for ($i = 0; $i < count($t); $i++) {
                foreach ($t[$i] as $k) {
                    $tables_real[] = $k;
                }
            }

            $tables_new = array();
            foreach ($databasePaths as $path) {
                if (file_exists($path) and is_dir($path)) {
                    $table_files = scandir($path);
                    foreach ($table_files as $fn) {
                        $fp = "$path/$fn";
                        if (is_file($fp)) {

                            // Importante: primeiro exibe o arquivo, depois as execuções dele
                            if (!$this->mute)
                                Mason::say("❍ Processing: " . realpath($fp), 'magenta');

                            $data = @yaml_parse(file_get_contents($fp));

                            if (!is_array($data)) {
                                if (!$this->mute)
                                    Mason::say("⚠ Invalid file format. Ignored.", 'yellow');
                                goto nextFile;
                            }

                            foreach ($data as $table_name => $table_cols) {
                                if (substr($table_name, 0, 1) === '~') {
                                    $table_name = $db_conf['PREF'] . substr($table_name, 1);
                                }

                                $tables_new[] = $table_name;

                                $field = $this->convertField($table_cols);
                                if (!$field)
                                    goto nextTable;

                                $ignore = @$table_cols['~ignore'];
                                if ($ignore)
                                    goto nextTable;

                                $field_curr = array();
                                if (in_array($table_name, $tables_real)) {
                                    // inclui column_default (necessário pra sync de DEFAULT)
                                    $r = $pg->query("SELECT column_name, data_type, is_nullable, character_maximum_length, column_default FROM information_schema.columns WHERE table_name = '$table_name'");
                                    if ($r[0]) {
                                        for ($x = 0; $x < count($r); $x++) {
                                            $field_curr[$r[$x]['column_name']] = $r[$x];
                                        }
                                        $this->updateTable($table_name, $field, $field_curr, $pg);
                                    } else {
                                        // Se por algum motivo não conseguiu ler colunas, ainda assim mantém padrão visual
                                        $this->headerTable($table_name);
                                        $this->sayUpToDate($table_name);
                                    }
                                } else {
                                    $this->createTable($table_name, $field, $pg);
                                }

                                nextTable:
                            }

                            nextFile:
                        }
                    }
                }
            }

            foreach ($tables_real as $k) {
                if (!in_array($k, $tables_new))
                    $this->deleteTable($k, $pg);
            }

            execute:
            if (!empty($this->queries)) {
                Mason::say("→ {$this->actions} requested actions for: $db_id");
                Mason::say("→ Please, verify:");

                // No resumo final: SEMPRE mini (nunca comando completo)
                for ($z = 0; $z < count($this->queries); $z++) {
                    $qr = $this->queries_mini[$z] ?? $this->miniFromSql($this->queries[$z]);
                    $color = $this->queries_color[$z] ?? 'cyan';
                    Mason::say("→ $qr", $color);
                }

                echo PHP_EOL;
                echo "Are you sure you want to do this? ☝" . PHP_EOL;
                echo "0: No" . PHP_EOL;
                echo "1: Yes" . PHP_EOL;
                echo "Choose an option: ";

                $handle = fopen("php://stdin", "r");
                $line = fgets($handle);
                fclose($handle);

                if (trim($line) == 0) {
                    echo "Aborting!" . PHP_EOL;
                    goto next_tenant;
                }

                for ($z = 0; $z < count($this->queries); $z++) {
                    $pg->query($this->queries[$z]);
                }
            }

            Mason::header("❤ Finished $db_id. Changes: {$this->actions}");
            next_tenant:
        }

        if ($this->create_database_count > 0) {
            Mason::header("Possible new databases: {$this->create_database_count}. Reloading...", 'cyan');
            $this->create_database_count = 0;
            $this->up(['--mute' => true]);
        }
    }
}
