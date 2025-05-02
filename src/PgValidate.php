<?php
class PgValidate extends Controllers
{
    // VALIDATE INPUT FIELDS
    public static function sanitize($receivedData, $requiredFields = false)
    {
        global $_APP;

        // CHECK REQUIRED FIELDS
        if ($requiredFields) {
            if (is_array($requiredFields)) $fields = $requiredFields;
            else $fields = explode(",", $requiredFields);
            foreach ($fields as $field) {
                $field = trim($field);
                if (!@$receivedData[$field]) Http::die(400, "Missing required field: $field");
            }
        }

        // SANITIZE FIELDS
        if (!$receivedData) return true;
        Xplend::load('PgValidate');
        $validatedData = $receivedData;

        // LOOP IN ALL DB FIELDS
        $fields = Database::getAllFields();
        foreach ($receivedData as $fieldName => $fieldValue) {
            // IS NOT DB FIELD
            if (!@$fields[$fieldName]) {
                $validatedData[$fieldName] = $fieldValue;
                continue;
            }
            if (!$fieldValue) continue;
            $params = explode(" ", $fields[$fieldName]);
            foreach ($params as $param) {
                $methodName = "validate_$param";
                if (method_exists('MyValidate', $methodName)) {
                    //echo "$methodName=> $fieldName=>" . MyValidate::$methodName($fieldValue) . "<br/>";
                    $validatedData[$fieldName] = MyValidate::$methodName($fieldValue);
                } elseif (function_exists($param)) {
                    $validatedData[$fieldName] = $param($fieldValue);
                }
                // validate error?
                if (@!empty($validatedData[$fieldName]['error'])) {
                    $validatedData['error'] = @$validatedData[$fieldName]['error'];
                    $validatedData['errors'][$fieldName] = @$validatedData[$fieldName]['error'];
                }
            }
        }
        return $validatedData;
    }
    // RETURN ERROR
    public static function fail($error, $data = [])
    {
        if (Xplend::isAPI()) Http::die(400, $error);
        else return ['error' => $error, 'data' => $data];
    }
    //-------------------------------------
    // ID
    //-------------------------------------
    public static function validate_id($data)
    {
        return intval($data);
    }
    //-------------------------------------
    // STR (MAX 64)
    //-------------------------------------
    public static function validate_str($data)
    {
        //if (strlen($data) > 64) return self::fail("String too long", $data);
        return $data;
    }
    //-------------------------------------
    // FLOAT
    //-------------------------------------
    public static function validate_float($data)
    {
        $comma = explode(",", $data);
        if (@$comma[1]) {
            $data = str_replace(".", "", $data);
            $data = str_replace(",", ".", $data);
        }
        return floatval($data);
    }
    // INT
    public static function validate_int($data)
    {
        return intval($data);
    }
    //-------------------------------------
    // UCWORDS (FNAME,LNAME)
    //-------------------------------------
    public static function validate_ucwords($data)
    {
        if (strlen($data) < 3) return self::fail("Name too short", $data);
        return ucwords(low($data));
    }
    //-------------------------------------
    // ALPHANUMERIC (CODE)
    //-------------------------------------
    public static function validate_alphanumeric($data)
    {
        if (strlen($data) > 64) return self::fail("String too long", $data);
        return alphanumeric($data);
    }
    //-------------------------------------
    // CHECK URL
    //-------------------------------------
    public static function validate_url($data)
    {
        // check string format
        $dots = explode(".", $data);
        if (!@$dots[1]) return self::fail("Invalid url: $data", $data);
        $prefix = explode("http://", $data);
        if (!@$prefix[1]) $data = "http://$data";
        // return
        $data = str_replace(['"', "'"], '', $data);
        $data = addslashes(low($data));
        return $data;
    }
    //-------------------------------------
    // CHECK EMAIL
    //-------------------------------------
    public static function validate_email($data)
    {
        // check string format
        if (!validaMail($data)) return self::fail("Invalid email format", $data);
        // check domain
        $domain = @explode("@", $data)[1];
        if (!checkdnsrr($domain, 'MX')) return self::fail("Invalid domain: $domain", $data);
        $data = low($data);
        return $data;
    }
    //-------------------------------------
    // CHECK CPF
    //-------------------------------------
    public static function validate_cpf($data)
    {
        if (!validaCPF($data)) return self::fail("Invalid CPF: $data", $data);
        $data = clean($data);
        return $data;
    }
    //-------------------------------------
    // CHECK CNPJ
    //-------------------------------------
    public static function validate_cnpj($data)
    {
        if (!validaCNPJ($data)) return self::fail("Invalid CNPJ: $data", $data);
        $data = clean($data);
        return $data;
    }
    //-------------------------------------
    // DATE
    //-------------------------------------
    public static function validate_date($data)
    {
        // check str. size
        $dateSizeCheck = false;
        if (strlen($data) === 10 or strlen($data) === 19) $dateSizeCheck = true;
        if (!$dateSizeCheck) return self::fail("Date invalid length", $data);
        // separate date
        $date = explode(' ', $data)[0];
        // time?
        $time = '00:00:00';
        if (@explode(' ', $data)[1] and @explode(':', $data)[1]) $time = explode(' ', $data)[1];
        // format br?
        if (@explode('/', $data)[1]) {
            $date = @explode('/', $date)[2] . '-' . @explode('/', $date)[1] . '-' . @explode('/', $date)[0];
        }
        // append time
        $data = "$date $time";
        return $data;
    }
    //-------------------------------------
    // PHONE
    //-------------------------------------
    public static function validate_phone($data)
    {
        $data = clean($data);
        if (strlen($data) !== 11) return self::fail("Phone invalid length", $data);
        return $data;
    }
}
