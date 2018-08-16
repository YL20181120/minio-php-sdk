<?php

namespace jasmine2\Minio;

use Nathanmac\Utilities\Parser\Parser;

/**
 * User: Jasmine2
 * Date: 2018/8/14 15:49
 * Email: youjingqiang@gmail.com
 */
class Client
{
    const DEFAULT_ENDPOINT = 's3.amazonaws.com';

    private $access_key;
    private $secret_key;
    private $endpoint;
    private $multi_curl;
    private $curl_opts;
    protected $secure;

    public function __construct($access_key, $secret_key, $endpoint = null)
    {
        $this->access_key = $access_key;
        $this->secret_key = $secret_key;
        $this->endpoint = $endpoint ?: self::DEFAULT_ENDPOINT;

        $this->multi_curl = curl_multi_init();

        $this->curl_opts = [
            CURLOPT_CONNECTTIMEOUT => 30,
            CURLOPT_LOW_SPEED_LIMIT => 1,
            CURLOPT_LOW_SPEED_TIME => 30
        ];
    }

    public function __destruct()
    {
        curl_multi_close($this->multi_curl);
    }

    public function useCurlOpts($curl_opts)
    {
        $this->curl_opts = $curl_opts;
        return $this;
    }

    public function putObject($bucket, $path, $file, $headers = [])
    {
        $uri = "$bucket/$path";

        $request = (new Request('PUT', $this->endpoint, $uri))
            ->setFileContents($file)
            ->setHeaders($headers)
            ->useMultiCurl($this->multi_curl)
            ->useCurlOpts($this->curl_opts)
            ->sign($this->access_key, $this->secret_key);

        return $request->getResponse();
    }

    public function getObjectInfo($bucket, $path, $headers = [])
    {
        $uri = "$bucket/$path";

        $request = (new Request('HEAD', $this->endpoint, $uri))
            ->setHeaders($headers)
            ->useMultiCurl($this->multi_curl)
            ->useCurlOpts($this->curl_opts)
            ->sign($this->access_key, $this->secret_key);

        return $request->getResponse();
    }

    public function getObject($bucket, $path, $resource = null, $headers = [])
    {
        $uri = "$bucket/$path";

        $request = (new Request('GET', $this->endpoint, $uri))
            ->setHeaders($headers)
            ->useMultiCurl($this->multi_curl)
            ->useCurlOpts($this->curl_opts)
            ->sign($this->access_key, $this->secret_key);

        if (is_resource($resource)) {
            $request->saveToResource($resource);
        }

        return $request->getResponse();
    }

    public function deleteObject($bucket, $path, $headers = [])
    {
        $uri = "$bucket/$path";

        $request = (new Request('DELETE', $this->endpoint, $uri))
            ->setHeaders($headers)
            ->useMultiCurl($this->multi_curl)
            ->useCurlOpts($this->curl_opts)
            ->sign($this->access_key, $this->secret_key);

        return $request->getResponse();
    }

    public function getBucket($bucket, $headers = [])
    {
        return $this->bucket('GET', $bucket, $headers);
    }

    public function listBucket($headers = [])
    {
        return $this->bucket('GET', '', $headers);
    }

    public function createBucket($bucket, $headers = [])
    {
        return $this->bucket('PUT', $bucket, $headers);
    }

    public function deleteBucket($bucket, $headers = [])
    {
        return $this->bucket('DELETE', $bucket, $headers);
    }

    protected function bucket($method = 'GET', $bucket = '', $headers = [])
    {
        $request = (new Request($method, $this->endpoint, $bucket))
            ->setHeaders($headers)
            ->useMultiCurl($this->multi_curl)
            ->useCurlOpts($this->curl_opts)
            ->sign($this->access_key, $this->secret_key);

        $response = $request->getResponse();

        if (!isset($response->error)) {
            $parser = new Parser();
            $body = $parser->xml($response->body);

            if ($body) {
                $response->body = $body;
            }
        }

        return $response;
    }
}