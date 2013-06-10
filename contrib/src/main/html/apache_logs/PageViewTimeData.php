<?php
header("Content-type: application/json");
$redis = new Redis();
$redis->connect('127.0.0.1');
$format = 'YmdHi';
$incr = 60;

// Get from date 
$from = $_GET['from'];
if (!$from || empty($from)) {
  $from  = time()-3600;
}

// get url   
$url = $_GET['url'];

// result array
$result = array();

while ($from < time()) 
{
  $date = gmdate($format, $from);
  if (!$url || empty($url))
  {
    $key =  'm|' . $date . '|*';
    $keys = $redis->getKeys($key);
    foreach($keys as &$val) 
    {
      $arr = $redis->hGetAll($val);
      $result[] = array("timestamp" => $from * 1000, "url" => $val, "view" => $arr[1]);
    }
  } else {
    
    $key =  'm|' . $date . '|0:' . $url;
    $arr = $redis->hGetAll($key);
    if ($arr)
    {
      $result[] = array("timestamp" => $from * 1000, "url" => $url, "view" => $arr[1]);
    }
  }
  $from += $incr;
}

print json_encode($result);

?>
