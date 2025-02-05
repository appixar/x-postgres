<?php
class postgres extends Mason
{
    public function __construct()
    {
        Mason::autoload($this);
    }
    public function up()
    {
        $argx = Mason::argx();
        //
        //Novel::module("POSTGRES");
        $schema = new PgBuilder();
        $schema->up($argx);
    }
    public function dump()
    {
        global $_APP;
        $host = $_APP['POSTGRES'][0]['HOST'];
        $name = $_APP['POSTGRES'][0]['NAME'];
        $user = $_APP['POSTGRES'][0]['USER'];
        $pass = $_APP['POSTGRES'][0]['PASS'];
        //
        $fn = time() . '-' . $name . '.sql';
        $fp = self::DIR_DB . $fn;
        //exec("POSTGRESdump --user=$user --password=$pass --host=$host --no-data $name > $fp");
        $this->say("* Generated: app/database/dump/<green>$fn</end>", true);
    }
}
