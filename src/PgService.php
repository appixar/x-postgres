<?php
class PgService extends Services
{
    public $con = array();
    public $conf = [];
    public $error = false;
    public $die = true; // die after query errors
    private static $instances = [];
    /*
    $conf = array(
        'die' => true,
        'id' => 0, // $_APP_VAULT[Postgres][0]
    );
    */
    public function __construct($conf = array())
    {
        $this->conf = $conf;
    }
    public function connect()
    {
        global $_APP_VAULT, $_ENV;
        if (!@$_APP_VAULT["POSTGRES"]) die('Postgres config is missing');

        // Get conf
        $conf = $this->conf;

        // Die after query errors?
        if (isset($conf['die'])) $this->die = $conf['die'];

        // Default connection ID = first
        $con_id = @$conf['db_key'];
        if (!$con_id) {
            foreach ($_APP_VAULT["POSTGRES"] as $k => $v) {
                $con_id = $k;
                break;
            }
        }

        // Connection data
        $pg = @$_APP_VAULT["POSTGRES"][$con_id];
        if (!$pg) die("Conn ID not found: $con_id");

        // Replace with env variables
        foreach ($pg as $k => $v) {
            if (!is_array($v) and substr($v, 0, 1) === '<' and substr($v, -1) === '>') {
                $v = substr($v, 1, -1);
                if (@!$_ENV[$v]) die("'$v' not found in .env");
                $pg[$k] = $_ENV[$v];
            }
        }

        // Wildcard variable replacement
        if (@$conf['tenant_key']) {
            foreach ($pg as $k => $v) {
                $pg[$k] = str_replace('<TENANT_KEY>', $conf['tenant_key'], $v);
            }
        }

        // Don't select database? (create if not exists after)
        $dbName = '';
        if (@!$conf['ignore-database']) $dbName = "dbname={$pg['NAME']}";

        // Unique identifier for connection configuration
        $uniqueId = md5(serialize($pg));

        // Check if an instance with the same configuration already exists
        if (isset(self::$instances[$uniqueId])) {
            return self::$instances[$uniqueId];
        }

        // Connect
        try {
            // Create new PDO connection instance
            $dsn = "pgsql:host={$pg['HOST']};{$dbName};port={$pg['PORT']}";
            $con = new PDO($dsn, $pg['USER'], $pg['PASS'], array(PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION));
            // Store the instance in the static property
            self::$instances[$uniqueId] = $con;
            return $con;
        } catch (PDOException $e) {
            die($e->getMessage());
        }
    }

    public function query($query, $variables = array())
    {
        if (!$this->con) $this->con = $this->connect();
        $stmt = $this->con->prepare($query);

        if ($variables) {
            $keys_find = explode(":", $query);
            unset($keys_find[0]);
            array_values($keys_find);
            foreach ($keys_find as $key) {
                $key = explode(" ", $key)[0];
                if (!is_numeric($key) && isAlphanumericOrUnderscore($key)) {
                    if (@$variables[$key]) $stmt->bindValue(":$key", $variables[$key]);
                    else die("Bind key not found ':$key'");
                }
            }
        }
        if (!$stmt->execute()) {
            if ($this->die) die($stmt->errorInfo()[2]);
            $this->error = $stmt->errorInfo()[2];
            return false;
        }
        $res = $stmt->fetchAll(PDO::FETCH_ASSOC);
        return $res;
    }

    public function insert($table, $data = array())
    {
        if (!$this->con) $this->con = $this->connect();

        $binds = array();

        $col = $val = $comma = "";
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

        $stmt = $this->con->prepare($query);

        foreach ($binds as $k => $v) $stmt->bindValue(":$k", $v);

        if (!$stmt->execute()) {
            if ($this->die) die($stmt->errorInfo()[2]);
            $this->error = $stmt->errorInfo()[2];
            return false;
        }

        try {
            $id = $this->con->lastInsertId();
            return $id;
        } catch (PDOException $e) {
            //die('Failed to get last insert ID: ' . $e->getMessage());
            return false;
        }
    }

    public function update($table, $data = array(), $condition = array())
    {
        if (!$this->con) $this->con = $this->connect();

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
        } else $where = $condition;

        $query = "UPDATE \"$table\" SET $values WHERE $where";

        $stmt = $this->con->prepare($query);

        foreach ($binds as $k => $v) $stmt->bindValue(":$k", $v);

        if (!$stmt->execute()) {
            if ($this->die) die($stmt->errorInfo()[2]);
            $this->error = $stmt->errorInfo()[2];
            return false;
        }
        return $stmt->rowCount();
    }
}
