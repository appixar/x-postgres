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

    // Adaptação dos tipos para PostgreSQL
    public $schema_default = array(
        'id' => array(
            'Type' => 'serial', // serial no PostgreSQL
            'Null' => 'NO',
            'Default' => '',
            'Key' => 'PRI',
            'Extra' => ''
        ),
        'str' => array(
            'Type' => 'varchar(64)',
            'Null' => 'YES',
            'Default' => '',
            'Key' => '',
            'Extra' => ''
        ),
        'date' => array(
            'Type' => 'timestamp', // timestamp no PostgreSQL
            'Null' => 'YES',
            'Default' => '',
            'Key' => '',
            'Extra' => ''
        ),
        'int' => array(
            'Type' => 'integer', // integer no PostgreSQL
            'Null' => 'YES',
            'Default' => '',
            'Key' => '',
            'Extra' => ''
        ),
        'float' => array(
            'Type' => 'real', // real no PostgreSQL
            'Null' => 'YES',
            'Default' => '',
            'Key' => '',
            'Extra' => ''
        ),
        'text' => array(
            'Type' => 'text', // text no PostgreSQL
            'Null' => 'YES',
            'Default' => '',
            'Key' => '',
            'Extra' => ''
        ),
        'email' => array(
            'Type' => 'varchar(128)',
            'Null' => 'YES',
            'Default' => '',
            'Key' => '',
            'Extra' => ''
        ),
        'ucwords' => array(
            'Type' => 'varchar(64)',
            'Null' => 'YES',
            'Default' => '',
            'Key' => '',
            'Extra' => ''
        ),
        'phone' => array(
            'Type' => 'varchar(11)',
            'Null' => 'YES',
            'Default' => '',
            'Key' => '',
            'Extra' => ''
        ),
        'cpf' => array(
            'Type' => 'varchar(11)',
            'Null' => 'YES',
            'Default' => '',
            'Key' => '',
            'Extra' => ''
        ),
        'cnpj' => array(
            'Type' => 'varchar(14)',
            'Null' => 'YES',
            'Default' => '',
            'Key' => '',
            'Extra' => ''
        ),
        'alphanumeric' => array(
            'Type' => 'varchar(64)',
            'Null' => 'YES',
            'Default' => '',
            'Key' => '',
            'Extra' => ''
        ),
        'url' => array(
            'Type' => 'text',
            'Null' => 'YES',
            'Default' => '',
            'Key' => '',
            'Extra' => ''
        ),
    );

    // Instância única para acesso a variáveis via método estático
    private static $instance = null;

    public function __construct()
    {
        if (!is_writable(self::DIR_SCHEMA)) {
            // die('ERROR:' . realpath(self::DIR_SCHEMA) . ' is not writable.' . PHP_EOL);
        }
    }

    public static function getInstance()
    {
        if (self::$instance === null) self::$instance = new self();
        return self::$instance;
    }

    public static function getParams()
    {
        return self::getInstance()->schema_default;
    }

    //-------------------------------------------------------
    // CONVERTE CAMPOS YML PARA FORMATO PostgreSQL
    //-------------------------------------------------------
    private function convertField($field)
    {
        $new_field = array();
        if (!is_array($field)) goto convertFieldEnd;

        foreach ($field as $k => $v) {
            // Separar tipo e index/unique se houver
            $parts = explode(" ", $v);
            $type_part = $parts[0];
            $type = explode("/", $type_part)[0];
            $type_real = @$this->schema_default[$type]['Type'];

            if (!$type_real) {
                $type_real = $type;
                $this->schema_default[$type_real] = [
                    'Type' => '',
                    'Null' => '',
                    'Default' => '',
                    'Extra' => ''
                ];
            }

            // Comprimento do campo, se especificado
            $len = @explode("/", $type_part)[1];
            if ($len) {
                $type_real = "$type_real($len)";
            }

            // Para campos SERIAL (id), não definir NULL ou NOT NULL
            if ($type === 'id' || strpos($type_real, 'SERIAL') !== false) {
                $null = '';
            } else {
                // Checar se é campo NOT NULL ou NULL
                $req = array_search('required', $parts);
                $null = ($req !== false) ? "NOT NULL" : "NULL";
            }

            // Verificar se tem UNIQUE
            $uni = array_search('unique', $parts);
            $key = ($uni !== false) ? 'UNI' : '';

            // Verificar se tem INDEX e se tem nome personalizado
            $index_name = '';
            foreach ($parts as $part) {
                if (strpos($part, 'index') !== false) {
                    $index_parts = explode("/", $part);
                    if (isset($index_parts[1])) {
                        $index_name = $index_parts[1];  // Nome do identificador do índice
                    }
                }
            }

            $new_field[$k] = array(
                'Field' => $k,
                'Type' => strtoupper($type_real),
                'Null' => $null,
                'Key' => $key,
                'IndexName' => $index_name,  // Nome do identificador do índice, se houver
                'Extra' => strtoupper(@$this->schema_default[$type]['Extra']),
            );
        }

        convertFieldEnd:
        return $new_field;
    }



    //-------------------------------------------------------
    // CREATE TABLE : EXECUTA A QUERY
    //-------------------------------------------------------
    private function createTable($table, $field, $pg)
    {
        if (!$this->mute) Mason::say("∴ $table", true, 'blue');
        $_comma = '';
        $query = "CREATE TABLE \"$table\" (" . PHP_EOL;

        $unique_fields = [];
        $index_fields = [];

        foreach ($field as $k => $v) {
            // FIELD PARAMETERS
            $type = strtoupper($v['Type']);
            $null = ($v['Null'] === 'NOT NULL') ? "NOT NULL" : ($v['Null'] === '' ? '' : "NULL");
            $extra = strtoupper(@$v['Extra']);

            // Evitar o uso de NULL para tipos como SERIAL
            if (strpos($type, 'SERIAL') !== false) {
                $null = ''; // SERIAL não aceita NULL ou NOT NULL
            }

            $query .= $_comma . "\"$k\" $type $null $extra";

            // Definir PRIMARY KEY
            if (@$v['Key'] === 'PRI') {
                $query .= ", PRIMARY KEY (\"$k\")";
            }

            // Coletar campos UNIQUE
            if (@$v['Key'] === 'UNI') {
                $unique_fields[] = $k;
            }

            // Coletar campos para INDEX
            if (@$v['Key'] === 'MUL') {
                $index_fields[] = $k;
            }

            $_comma = ', ' . PHP_EOL;
        }

        $query .= PHP_EOL . ");";
        if (!$this->mute) Mason::say("→ $query", false, 'green');
        $this->queries[] = $query;
        $this->queries_mini[] = "CREATE TABLE \"$table\" ...";
        $this->queries_color[] = 'green';
        $this->actions++;

        // Adicionar UNIQUE constraints após criação da tabela
        foreach ($unique_fields as $unique_field) {
            $query = "ALTER TABLE \"$table\" ADD CONSTRAINT \"{$table}_{$unique_field}_unique\" UNIQUE (\"$unique_field\");";
            $this->queries[] = $query;
            $this->queries_mini[] = "CREATE UNIQUE \"$unique_field\" ...";
            $this->queries_color[] = 'cyan';
            if (!$this->mute) Mason::say("→ $query", false, 'cyan');
            $this->actions++;
        }

        // Criar índices após a criação da tabela
        foreach ($index_fields as $index_field) {
            $query = "CREATE INDEX \"{$table}_{$index_field}_idx\" ON \"$table\" (\"$index_field\");";
            $this->queries[] = $query;
            $this->queries_mini[] = "CREATE INDEX \"$index_field\" ...";
            $this->queries_color[] = 'cyan';
            if (!$this->mute) Mason::say("→ $query", false, 'cyan');
            $this->actions++;
        }
    }

    //-------------------------------------------------------
    // UPDATE TABLE : EXECUTA A QUERY
    //-------------------------------------------------------
    private function updateTable($table, $field, $field_curr, $pg)
    {
        if (!$this->mute) Mason::say("∴ $table", true, 'blue');
        $query = '';

        // REMOVE CAMPOS
        foreach ($field_curr as $k => $v) {
            if (!@$field[$k]) {
                $query = "ALTER TABLE \"$table\" DROP COLUMN \"$k\";";
                $this->queries[] = $query;
                $this->queries_color[] = 'yellow';
                if (!$this->mute) Mason::say("→ $query", false, 'yellow');
                $this->actions++;
            }
        }

        // CRIAR + ATUALIZAR CAMPOS
        foreach ($field as $k => $v) {
            if (!@$field_curr[$k]) {
                $query = "ALTER TABLE \"$table\" ADD COLUMN \"$k\" " . strtoupper($v['Type']) . " " . $v['Null'] . " " . $v['Extra'] . ";";
                $this->queries[] = $query;
                $this->queries_color[] = 'green';
                if (!$this->mute) Mason::say("→ $query", false, 'green');
                $this->actions++;
            }
        }
    }

    //-------------------------------------------------------
    // DELETE TABLE : EXECUTA A QUERY
    //-------------------------------------------------------
    private function deleteTable($table, $pg)
    {
        if (!$this->mute) Mason::say("∴ $table", true, 'blue');
        $query = "DROP TABLE IF EXISTS \"$table\" CASCADE;";
        if (!$this->mute) Mason::say("→ $query", false, 'yellow');
        $this->queries[] = $query;
        $this->queries_color[] = 'yellow';
        $this->actions++;
    }

    //-------------------------------------------------------
    // CREATE DATABASE : EXECUTA A QUERY
    //-------------------------------------------------------
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

    //-------------------------------------------------------
    // REVERSO PARA OBTER A ESTRUTURA DA TABELA
    //-------------------------------------------------------
    public function buildReverse()
    {
        $table = array();
        $r = jwquery("SELECT table_name FROM information_schema.tables WHERE table_schema='public'");
        for ($i = 0; $i < count($r); $i++) {
            foreach ($r[$i] as $k => $v) $table[] = $v;
        }
        for ($i = 0; $i < count($table); $i++) {
            $field = array();
            $r = jwquery("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{$table[$i]}'");
            for ($x = 0; $x < count($r); $x++) {
                $f_name = $r[$x]['column_name'];
                $f_type = $r[$x]['data_type'];
                $f_null = $r[$x]['is_nullable'];
                // Preencher lógica com base na análise da estrutura
            }
        }
    }

    //-------------------------------------------------------
    // EXECUÇÃO PRINCIPAL: up() PARA GERAR OU ATUALIZAR DB/TABELAS
    //-------------------------------------------------------
    public function up($argx)
    {
        global $_APP;

        // sub --arguments
        if (@$argx['--mute']) $this->mute = true;
        if (@$argx['--create']) $this->create_database = true;
        if (@$argx['--name']) $this->select_database = $argx['--name'];
        if (@$argx['--tenant']) $this->select_tenant = $argx['--tenant'];

        if (!@is_array($_APP['POSTGRES'])) {
            Mason::say("Ops! config is missing.", false, "red");
            Mason::say("Please, verify: modules/postgres/config/postgres.yml", false, "red");
            exit;
        }

        foreach ($_APP['POSTGRES'] as $db_id => $db_conf) {

            if ($this->select_tenant) {
                if (!@$db_conf['TENANT_KEYS']) continue;
            }

            if ($this->select_database) {
                if ($this->select_database !== $db_conf['NAME'] and !@$db_conf['TENANT_KEYS']) {
                    continue;
                }
            }
            Mason::say("► PostgreSQL '$db_id' ...", true, 'cyan');

            // Diretórios de configuração de DB
            if (@$db_conf['PATH']) {
                if (!is_array($db_conf['PATH'])) $db_conf['PATH'] = [$db_conf['PATH']];
                for ($i = 0; $i < count($db_conf['PATH']); $i++) {
                    $db_conf['PATH'][$i] = realpath(__DIR__ . '/../../../' . $db_conf['PATH'][$i] . '/');
                }
                $databasePaths = $db_conf['PATH'];
            } else $databasePaths = Xplend::findPathsByType("database");

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
            for ($i = 0; $i < count($t); $i++) foreach ($t[$i] as $k) $tables_real[] = $k;

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
                                    $r = $pg->query("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '$table_name'");
                                    if ($r[0]) {
                                        for ($x = 0; $x < count($r); $x++) $field_curr[$r[$x]['column_name']] = $r[$x];
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

            // CONFIRM CHANGES
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

                //----------------------------------------------
                // EXECUTE QUERIES!
                //----------------------------------------------
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
