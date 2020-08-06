<?php

/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace MakiseCo\Database\Driver\MakisePostgres;

use function array_key_exists;
use function count;
use function explode;
use function in_array;
use function strpos;
use function substr;

final class OptionsProcessor
{
    private function __construct()
    {
    }

    public static function cleanOptions(array $options): array
    {
        // parse DSN
        if (array_key_exists('connection', $options) && !empty($options['connection'])) {
            static::parseDsn($options['connection'], $options);
        } elseif (array_key_exists('dsn', $options) && !empty($options['dsn'])) {
            static::parseDsn($options['dsn'], $options);
        } elseif (array_key_exists('addr', $options) && !empty($options['addr'])) {
            static::parseDsn($options['addr'], $options);
        }

        // alias pass field to password field
        if (array_key_exists('pass', $options)) {
            $options['password'] = $options['pass'];
        }

        // clean forbidden options
        foreach ($options as $key => $value) {
            if (!in_array($key, self::ALLOWED_KEYS, true)) {
                unset($options[$key]);
            }
        }

        return $options;
    }

    private static function parseDsn(string $dsn, array &$options): void
    {
        // cut PDO DSN prefix
        if (0 === strpos($dsn, 'pgsql:')) {
            $dsn = substr($dsn, 6);
        }

        $connParts = explode(';', $dsn);

        if (count($connParts) === 1) { // Attempt to explode on a space if no ';' are found.
            $connParts = explode(' ', $dsn);
        }

        foreach ($connParts as $connPart) {
            [$key, $value] = explode('=', $connPart, 2);

            if (!array_key_exists($key, $options)) {
                if ($key === 'port') {
                    $value = (int)$value;
                } elseif ($key === 'unbuffered') {
                    $value = (bool)$value;
                }

                $options[$key] = $value;
            }
        }
    }

    private const ALLOWED_KEYS = [
        'host',
        'port',
        'user',
        'username',
        'password',
        'dbname',
        'database',
        'application_name',
        'timezone',
        'encoding',
        'charset',
        'schema',
        'search_path',
        'connect_timeout',
        'unbuffered',
        'options',
    ];
}