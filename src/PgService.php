<?php
class PgService extends Services
{
    /* $conf = [
        'cluster' => 'clusterName', # nome do cluster (chave do array da config dos BDs)
        'ignore_dbname' => true,    # ignorar seleção do nome do BD ao se conectar
        'primary' => true           # caso esteja em modo múltiplas réplicas, forçar execução no primário
    ]
    */
    public $conf = [];
    public $con = null;
    public $conData = [];
    public $conWrite = null;
    public $conRead = null;
    public $error = false;
    private $die = true; // die after query errors
    private static $instances = [];
    public function __construct($conf = array())
    {
        $this->conf = $conf;
    }
    public function connect($currentType = 'read')
    {
        global $_APP_VAULT;

        if (!isset($_APP_VAULT["POSTGRES"]["DB"])) die('Postgres config is missing');

        // Configuração da conexão
        $conf = $this->conf;
        if (isset($conf['die'])) $this->die = $conf['die'];

        // Obter nome do cluster
        $clusterName = $conf['cluster'] ?? array_key_first($_APP_VAULT["POSTGRES"]["DB"]);
        if (!$clusterName || !isset($_APP_VAULT["POSTGRES"]["DB"][$clusterName])) {
            Xplend::err("Postgres", "Database cluster not found: $clusterName");
        }
        // Dados do cluster
        $clusterList = $_APP_VAULT["POSTGRES"]["DB"][$clusterName];
        if (!is_array($clusterList)) Xplend::err("Postgres", "Config format error");
        $clusterData = [];
        $clusterWrite = []; // armazenar será útil para checar se o usuário quer forçar execução no node primário

        // Multiplos DB no cluster
        // Obter dados de leitura e escrita
        $isMulti = false;
        if (array_keys($clusterList) === range(0, count($clusterList) - 1)) $isMulti = true;
        // Múltiplos Nodes neste cluster
        if ($isMulti) {
            // Verificar se 'type' foi definido corretamente em todos os DB
            // ... e apenas um pode ser 'write'
            $writeCount = 0;
            foreach ($clusterList as $k => $v) {
                if (!@$v['TYPE']) Xplend::err("Postgres", "Type can't be null on cluster '$clusterName'");
                if (!in_array(@$v['TYPE'], ['write', 'read'])) Xplend::err("Postgres", "Wrong type '{$v['TYPE']}' on cluster '$clusterName'");
                if (@$v['TYPE'] == 'write') {
                    $writeCount++;
                    $clusterWrite = $v;
                    if ($currentType == 'write') $clusterData = $v;
                } else {
                    // Opção 1: Array com múltiplos HOSTS por NODE
                    $hostList = $v['HOST'];
                    if (is_array($hostList)) {
                        foreach ($hostList as $host) {
                            $node = $v;
                            $node['HOST'] = $host;
                            $clusterReadOptions[] = $node;
                        }
                    }
                    // Opção 2: Um HOST por NODE
                    else $clusterReadOptions[] = $v;
                }
            }
            if ($writeCount > 1) Xplend::err("Postgres", "Only one DB can be 'write' type");
            // Escolher um BD para read
            if ($currentType == 'read') {
                $randomIndex = array_rand($clusterReadOptions);
                #echo "index read=" . $randomIndex . PHP_EOL;
                $clusterData = $clusterReadOptions[$randomIndex];
            }
        }
        // Único NODE neste cluster
        else $clusterData = $clusterList;
        #prex($clusterData);

        // Forçar execução no node primário? (node escrita)
        if (@$conf['primary'] and $isMulti) {
            $clusterData = $clusterWrite;
            $currentType = 'write';
        }

        // Se a conexão já existe, reutilizar
        if (isset(self::$instances[$clusterName][$currentType])) {
            #echo "ja existe, aproveitar: $currentType" . PHP_EOL;
            // Current con for current query
            $this->con = self::$instances[$clusterName][$currentType];
            return;
        }
        // Criar conexão
        try {
            $dbName = isset($conf['ignore_dbname']) ? '' : "dbname={$clusterData['NAME']}";
            $dsn = "pgsql:host={$clusterData['HOST']};{$dbName};port={$clusterData['PORT']}";
            $pdo = new PDO($dsn, $clusterData['USER'], $clusterData['PASS'], [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_PERSISTENT => true
            ]);

            // Armazena a conexão para reutilização
            self::$instances[$clusterName][$currentType] = $pdo;
            // Conexão atual
            $this->con = $pdo;
            $this->conData = $clusterData;
            // Conexões write & read, para próximas queries
            if ($currentType == 'write') $this->conWrite = $pdo;
            else $this->conRead = $pdo;

            #echo "nova conexao: $currentType" . PHP_EOL;
        } catch (PDOException $e) {
            die("PostgreSQL Connection Error: " . $e->getMessage());
        }
    }
    private function getConnection($currentType = 'read')
    {
        if ($currentType == 'write' and $this->conWrite) $this->con = $this->conWrite;
        elseif ($currentType == 'read' and $this->conRead) $this->con = $this->conRead;
        else $this->connect($currentType);
    }

    public function query($query, $variables = array(), $cacheTimer = 0)
    {
        // CACHE FIRST
        if ($cacheTimer > 0) {
            $cache = new Cache();
            $cacheKey = md5($query . json_encode($variables));
            $res = $cache->get($cacheKey);
            if ($res) {
                #echo 'reading from cache...' . PHP_EOL;
                return $res;
            }
        }
        try {
            // SELECT CONNECTION
            $type = 'write';
            if ($this->isReadOnlyQuery($query)) $type = 'read';
            $this->getConnection($type);
            $stmt = $this->con->prepare($query);

            if ($variables) {
                $keys_find = explode(":", $query);
                unset($keys_find[0]);
                array_values($keys_find);
                foreach ($keys_find as $key) {
                    $key = trim(explode(" ", $key)[0]);
                    if (!is_numeric($key) && isAlphanumericOrUnderscore($key)) {
                        if (isset($variables[$key])) $stmt->bindValue(":$key", $variables[$key]);
                        else Xplend::err("Postgres", "Bind key not found ':$key'");
                    }
                }
            }
            if (!$stmt->execute()) {
                if ($this->die) die($stmt->errorInfo()[2]);
                $this->error = $stmt->errorInfo()[2];
                return false;
            }
            // RESULT
            $res = $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (PDOException $e) {
            if (Xplend::isAPI()) Http::die(500, $e->getMessage());
            else Xplend::err("Postgres", $e->getMessage());
        }

        // SAVE IN CACHE
        if ($cacheTimer > 0) {
            #echo 'saving on cache...' . PHP_EOL;
            $cache->set($cacheKey, $res, $cacheTimer);
        }
        // RETURN
        return $res;
    }
    public function getTableFields($table)
    {
        $path = @$this->conData['PATH'];
        $pref = @$this->conData['PREF'];
        if ($path) {
            if (!is_array($path)) $path = [$path];
            for ($i = 0; $i < count($path); $i++) {
                $path[$i] = realpath(__DIR__ . '/../../../' . $path[$i] . '/');
            }
            $databasePaths = $path;
        } else $databasePaths = Xplend::findPathsByType("database");

        foreach ($databasePaths as $path) {
            if (file_exists($path) and is_dir($path)) {
                $table_files = scandir($path);
                foreach ($table_files as $fn) {
                    $fp = "$path/$fn";
                    if (is_file($fp)) {
                        $data = @yaml_parse(file_get_contents($fp));
                        if (!is_array($data)) continue;
                        foreach ($data as $table_name => $table_cols) {
                            if (substr($table_name, 0, 1) === '~') {
                                $table_name = $pref . substr($table_name, 1);
                            }
                            if ($table_name == $table) return $table_cols;
                        }
                    }
                }
            }
        }
    }
    public function replaceGenerateFields($table, $data)
    {
        $fields = $this->getTableFields($table);
        if (!empty($fields)) {
            foreach ($fields as $fieldName => $fieldConf) {
                $parts = explode(" ", $fieldConf);
                $fieldType = $parts[0];
                if (in_array('generate', $parts)) {
                    if (!function_exists('generate_' . $fieldType)) {
                        Xplend::err('Postgres', "Generate function not found: generate_$fieldType()");
                    } else {
                        $generateData = [
                            'table' => $table,
                            'data' => $data,
                            'field_name' => $fieldName,
                            'field_data' => @$data[$fieldName]
                        ];
                        $data[$fieldName] = call_user_func("generate_" . $fieldType, $generateData);
                    }
                }
            }
        }
        return $data;
    }
    public function insert($table, $data = array())
    {
        $this->getConnection('write');
        $binds = array();
        $col = $val = $comma = "";

        // Replace generate fields
        $data = $this->replaceGenerateFields($table, $data);

        foreach ($data as $k => $v) {
            if ($v === "NULL" or $v === "null" or $v === '') $v = "NULL";
            elseif (is_numeric($v)) $v = "$v";
            else {
                $binds[$k] = $v;
                $v = ":$k";
            }
            $val .= "$comma$v";
            $col .= "$comma\"$k\"";
            $comma = ",";
        }

        $query = "INSERT INTO \"$table\" ($col) VALUES ($val)";

        try {
            $stmt = $this->con->prepare($query);
            foreach ($binds as $k => $v) $stmt->bindValue(":$k", $v);
            if (!$stmt->execute()) {
                if ($this->die) die($stmt->errorInfo()[2]);
                $this->error = $stmt->errorInfo()[2];
                return false;
            }
        } catch (PDOException $e) {
            Xplend::err("Postgres", $e->getMessage());
        }

        try {
            return $this->con->lastInsertId();
        } catch (PDOException $e) {
            return false;
        }
    }

    public function update($table, $data = array(), $condition = array())
    {
        $this->getConnection('write');
        $binds = array();
        $comma = $values = $and = $where = "";

        foreach ($data as $k => $v) {
            if ($v === "NULL" or $v === "null") $v = "NULL";
            elseif (is_numeric($v)) $v = "$v";
            elseif ($v === "") $v = "NULL";
            else {
                $binds[$k] = $v;
                $v = ":$k";
            }
            $values .= "$comma\"$k\"=$v";
            $comma = ",";
        }

        if (is_array($condition)) {
            foreach ($condition as $k => $v) {
                if ($v === "NULL") $where .= $and . "\"$k\" IS NULL";
                elseif ($v === "") $where .= $and . "\"$k\" = ''";
                elseif (is_numeric($v)) $where .= $and . "\"$k\" = '$v'";
                else {
                    $where .= $and . "\"$k\" = :$k";
                    $binds[$k] = $v;
                }
                $and = " AND ";
            }
        } else {
            $where = $condition;
        }

        $query = "UPDATE \"$table\" SET $values WHERE $where";
        try {
            $stmt = $this->con->prepare($query);
            foreach ($binds as $k => $v) $stmt->bindValue(":$k", $v);
            if (!$stmt->execute()) {
                if ($this->die) die($stmt->errorInfo()[2]);
                $this->error = $stmt->errorInfo()[2];
                return false;
            }
        } catch (PDOException $e) {
            Xplend::err("Postgres", $e->getMessage());
        }

        return $stmt->rowCount();
    }
    private function isReadOnlyQuery($query)
    {
        $query = trim($query);
        $queryType = strtoupper(strtok($query, " ")); // Obtém a primeira palavra da query
        $readOnlyCommands = ['SELECT', 'SHOW', 'EXPLAIN', 'WITH RECURSIVE'];
        return in_array($queryType, $readOnlyCommands);
    }
}
