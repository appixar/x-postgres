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
        'SERIAL'    => 'integer',
        'VARCHAR'   => 'character varying',
        'INT'       => 'integer',
        'INTEGER'   => 'integer',
        'TEXT'      => 'text',
        'TIMESTAMP' => 'timestamp without time zone',
        'DATE'      => 'date',
        'TIME'      => 'time without time zone',
        'BOOLEAN'   => 'boolean',
        'SMALLINT'  => 'smallint',
        'BIGINT'    => 'bigint',
        'REAL'      => 'real',
        'DOUBLE'    => 'double precision',
        'NUMERIC'   => 'numeric',
        'DECIMAL'   => 'numeric',
        'JSON'      => 'json',
        'JSONB'     => 'jsonb',
        'UUID'      => 'uuid',
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

    private function convertField($field)
    {
        $new_field = array();
        $individual_indexes = [];
        $composite_indexes = [];

        if (!is_array($field)) goto convertFieldEnd;

        foreach ($field as $k => $v) {
            $parts = explode(" ", $v);

            // find field type
            $type_part = $parts[0];
            $type = explode("/", $type_part)[0];
            $type_from_custom = @$this->custom_fields[$type]['Type'];
            $type_real = '';
            if ($type_from_custom) $type_real = $type_from_custom;
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
            if (array_search('unique', $parts) !== false) $key = 'UNI';
            if ($key_from_custom) $key = $key_from_custom;

            // Define indexes
            foreach ($parts as $part) {
                if (strpos($part, 'index') !== false) {
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
            }

            $new_field[$k] = array(
                'Field' => $k,
                'Type' => strtoupper($type_real),
                'Null' => $null,
                'Key' => $key,
                'Extra' => @strtoupper(@$this->custom_fields[$type]['Extra']),
            );
        }

        convertFieldEnd:
        return [
            'fields' => $new_field,
            'individual_indexes' => array_unique($individual_indexes),
            'composite_indexes' => $composite_indexes
        ];
    }

    private function createTable($table, $schema, $pg)
    {
        if (!$this->mute) Mason::say("∴ $table", true, 'blue');

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

            $query .= $_comma . "\"$k\" $type $null $extra";

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
        $query = removeExtraSpaces($query);
        if (!$this->mute) Mason::say("→ $query", false, 'green');
        $this->queries[] = $query;
        $this->queries_mini[] = "CREATE TABLE \"$table\" ...";
        $this->queries_color[] = 'green';
        $this->actions++;

        foreach ($unique_fields as $unique_field) {
            $query = "ALTER TABLE \"$table\" ADD CONSTRAINT \"{$table}_{$unique_field}_unique\" UNIQUE (\"$unique_field\");";
            $this->queries[] = $query;
            $this->queries_mini[] = "ADD UNIQUE \"{$table}_{$unique_field}_unique\" ...";
            $this->queries_color[] = 'cyan';
            if (!$this->mute) Mason::say("→ $query", false, 'cyan');
            $this->actions++;
        }

        foreach ($index_fields as $index_field) {
            $query = "CREATE INDEX \"{$table}_{$index_field}_idx\" ON \"$table\" (\"$index_field\");";
            $this->queries[] = $query;
            $this->queries_mini[] = "ADD INDEX \"{$table}_{$index_field}_idx\" ...";
            $this->queries_color[] = 'cyan';
            if (!$this->mute) Mason::say("→ $query", false, 'cyan');
            $this->actions++;
        }

        foreach ($composite_indexes as $index_name => $columns) {
            $columns_str = implode('", "', $columns);
            $query = "CREATE INDEX \"{$table}_{$index_name}_idx\" ON \"$table\" (\"$columns_str\");";
            $this->queries[] = $query;
            $this->queries_mini[] = "ADD INDEX \"{$table}_{$index_name}_idx\" ...";
            $this->queries_color[] = 'cyan';
            if (!$this->mute) Mason::say("→ $query", false, 'cyan');
            $this->actions++;
        }
    }

    private function updateTable($table, $schema, $field_curr, $pg)
    {
        if (!$this->mute) Mason::say("∴ $table", true, 'blue');

        $fields = $schema['fields'];
        $individual_indexes = $schema['individual_indexes'];
        $composite_indexes = $schema['composite_indexes'];

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
                $query = "ALTER TABLE \"$table\" DROP COLUMN \"$column\";";
                $this->queries[] = $query;
                $this->queries_color[] = 'yellow';
                $this->actions++;
                if (!$this->mute) Mason::say("→ $query", false, 'yellow');
            }
        }

        // Remove UNIQUE constraints not in configuration
        foreach ($existing_unique_names as $unique_name) {
            if (!in_array($unique_name, $expected_unique_names)) {
                $query = "ALTER TABLE \"$table\" DROP CONSTRAINT \"$unique_name\";";
                $this->queries[] = $query;
                $this->queries_color[] = 'yellow';
                $this->actions++;
                if (!$this->mute) Mason::say("→ $query", false, 'yellow');
            }
        }

        // Remove indexes not in configuration
        foreach ($existing_index_names as $index_name) {
            if (!in_array($index_name, $expected_indexes)) {
                $query = "DROP INDEX IF EXISTS \"$index_name\";";
                $this->queries[] = $query;
                $this->queries_color[] = 'yellow';
                $this->actions++;
                if (!$this->mute) Mason::say("→ $query", false, 'yellow');
            }
        }

        // Add new columns
        foreach ($fields as $k => $v) {
            if (!isset($field_curr[$k])) {
                $query = "ALTER TABLE \"$table\" ADD COLUMN \"$k\" " . strtoupper($v['Type']) . " " . $v['Null'] . " " . $v['Extra'] . ";";
                $this->queries[] = $query;
                $this->queries_color[] = 'cyan';
                $this->actions++;
                if (!$this->mute) Mason::say("→ $query", false, 'cyan');
            }
        }

        // Update existing columns if field length differs
        // Update existing columns if field type or length differs
        foreach ($fields as $k => $v) {
            if (!isset($field_curr[$k])) continue;

            // Extract configured base type and length (if provided)
            if (preg_match('/^(\w+)(?:\((\d+)\))?$/', $v['Type'], $matches)) {
                $configBaseType = strtoupper($matches[1]);
                $configLength   = isset($matches[2]) ? (int)$matches[2] : null;
            } else {
                $configBaseType = @explode("(", strtoupper($v['Type']))[0];
                $configLength   = null;
            }

            // Map the config base type using the dictionary
            if (isset($this->postgresTypeDictionary[$configBaseType])) {
                $mappedConfigType = $this->postgresTypeDictionary[$configBaseType];
            } else {
                $mappedConfigType = strtolower($configBaseType);
            }

            // Get the current database field type and length
            $dbType   = strtolower($field_curr[$k]['data_type']);
            $dbLength = isset($field_curr[$k]['character_maximum_length']) ? (int)$field_curr[$k]['character_maximum_length'] : null;

            // If the base type is different, update with the new type and length (if provided)
            if ($mappedConfigType !== $dbType) {
                $query = "ALTER TABLE \"$table\" ALTER COLUMN \"$k\" TYPE " . strtoupper($v['Type']) . ";";
                $this->queries[] = $query;
                $this->queries_color[] = 'cyan';
                $this->actions++;
                if (!$this->mute) Mason::say("→ $query", false, 'cyan');
            }
            // If the type is the same but the length differs, update the column type with the new length
            else if ($configLength !== null && $dbLength !== $configLength) {
                $query = "ALTER TABLE \"$table\" ALTER COLUMN \"$k\" TYPE " . strtoupper($v['Type']) . ";";
                $this->queries[] = $query;
                $this->queries_color[] = 'cyan';
                $this->actions++;
                if (!$this->mute) Mason::say("→ $query", false, 'cyan');
            }
        }

        // Create individual indexes if not exists
        foreach ($individual_indexes as $index_field) {
            $index_name = "{$table}_{$index_field}_idx";
            if (!in_array($index_name, $existing_index_names)) {
                $query = "CREATE INDEX \"$index_name\" ON \"$table\" (\"$index_field\");";
                $this->queries[] = $query;
                $this->queries_color[] = 'cyan';
                $this->actions++;
                if (!$this->mute) Mason::say("→ $query", false, 'cyan');
            }
        }

        // Create composite indexes if not exists
        foreach ($composite_indexes as $index_name => $columns) {
            $index_name_full = "{$table}_{$index_name}_idx";
            if (!in_array($index_name_full, $existing_index_names)) {
                $columns_str = implode('", "', $columns);
                $query = "CREATE INDEX \"$index_name_full\" ON \"$table\" (\"$columns_str\");";
                $this->queries[] = $query;
                $this->queries_color[] = 'cyan';
                $this->actions++;
                if (!$this->mute) Mason::say("→ $query", false, 'cyan');
            }
        }

        // Create UNIQUE constraints if not exists
        foreach ($fields as $k => $v) {
            if (@$v['Key'] === 'UNI') {
                $unique_name = "{$table}_{$k}_unique";
                if (!in_array($unique_name, $existing_unique_names)) {
                    $query = "ALTER TABLE \"$table\" ADD CONSTRAINT \"$unique_name\" UNIQUE (\"$k\");";
                    $this->queries[] = $query;
                    $this->queries_color[] = 'cyan';
                    $this->actions++;
                    if (!$this->mute) Mason::say("→ $query", false, 'cyan');
                }
            }
        }
    }

    private function deleteTable($table, $pg)
    {
        if (!$this->mute) Mason::say("∴ $table", true, 'blue');
        $query = "DROP TABLE IF EXISTS \"$table\" CASCADE;";
        if (!$this->mute) Mason::say("→ $query", false, 'yellow');
        $this->queries[] = $query;
        $this->queries_color[] = 'yellow';
        $this->actions++;
    }

    private function createDatabase($name, $pg)
    {
        $query = "CREATE DATABASE \"$name\" ENCODING 'UTF8';";
        $this->queries[] = $query;
        $this->queries_mini[] = "CREATE DATABASE \"$name\"";
        $this->queries_color[] = 'green';
        $this->actions++;
        $this->create_database_count++;
        if (!$this->mute) Mason::say("→ $query", false, 'green');
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

        if (@$argx['--mute']) $this->mute = true;
        if (@$argx['--create']) $this->create_database = true;
        if (@$argx['--name']) $this->select_database = $argx['--name'];
        if (@$argx['--tenant']) $this->select_tenant = $argx['--tenant'];

        if (!@is_array($_APP['POSTGRES']['DB'])) {
            Mason::say("Ops! config is missing.", false, "red");
            Mason::say("Please, verify: modules/postgres/config/postgres.yml", false, "red");
            exit;
        }

        foreach ($_APP['POSTGRES']['DB'] as $db_id => $db_conf) {
            if ($this->select_tenant) {
                if (!@$db_conf['TENANT_KEYS']) continue;
            }

            if ($this->select_database) {
                if ($this->select_database !== $db_conf['NAME'] and !@$db_conf['TENANT_KEYS']) {
                    continue;
                }
            }
            Mason::say("► PostgreSQL '$db_id' ...", true, 'cyan');

            if (@$db_conf['PATH']) {
                if (!is_array($db_conf['PATH'])) $db_conf['PATH'] = [$db_conf['PATH']];
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
                            if (!$this->mute) Mason::say("⍐ Processing: " . realpath($fp), false, 'magenta');

                            $data = @yaml_parse(file_get_contents($fp));

                            if (!is_array($data)) {
                                if (!$this->mute) Mason::say("* Invalid file format. Ignored.", false, 'yellow');
                                goto nextFile;
                            }

                            foreach ($data as $table_name => $table_cols) {
                                if (substr($table_name, 0, 1) === '~') {
                                    $table_name = $db_conf['PREF'] . substr($table_name, 1);
                                }

                                $tables_new[] = $table_name;

                                $field = $this->convertField($table_cols);
                                if (!$field) goto nextTable;

                                $ignore = @$table_cols['~ignore'];
                                if ($ignore) goto nextTable;

                                $field_curr = array();
                                if (in_array($table_name, $tables_real)) {
                                    // Note the inclusion of character_maximum_length for field length comparison
                                    $r = $pg->query("SELECT column_name, data_type, is_nullable, character_maximum_length FROM information_schema.columns WHERE table_name = '$table_name'");
                                    if ($r[0]) {
                                        for ($x = 0; $x < count($r); $x++) {
                                            $field_curr[$r[$x]['column_name']] = $r[$x];
                                        }
                                        $this->updateTable($table_name, $field, $field_curr, $pg);
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
                if (!in_array($k, $tables_new)) $this->deleteTable($k, $pg);
            }

            execute:
            if (!empty($this->queries)) {
                Mason::say("→ {$this->actions} requested actions for: $db_id");
                Mason::say("→ Please, verify:");
                for ($z = 0; $z < count($this->queries); $z++) {
                    $qr = @$this->queries_mini[$z] ? $this->queries_mini[$z] : $this->queries[$z];
                    Mason::say("→ $qr", false, $this->queries_color[$z]);
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

            Mason::say("❤ Finished $db_id. Changes: {$this->actions}", true, 'header');
            next_tenant:
        }

        if ($this->create_database_count > 0) {
            Mason::say("Possible new databases: {$this->create_database_count}. Reloading...", true, 'cyan');
            $this->create_database_count = 0;
            $this->up(['--mute' => true]);
        }
    }
}
