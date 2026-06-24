CREATE TABLE IF NOT EXISTS `endpoints-output-discord` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL DEFAULT '',
  `webhook_url` VARCHAR(2048) NOT NULL DEFAULT '',
  `status` ENUM('New', 'Unchecked', 'Offline', 'Online') NOT NULL DEFAULT 'Unchecked',
  `mention_text` VARCHAR(255) NOT NULL DEFAULT '',
  `username` VARCHAR(80) NOT NULL DEFAULT '',
  `avatar_url` VARCHAR(2048) NOT NULL DEFAULT '',
  `exclude_bells` TINYINT(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (`id`),
  KEY `status_idx` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE IF NOT EXISTS `endpoints-modulesettings-discord` (
  `parameter` VARCHAR(128) NOT NULL,
  `value` TEXT,
  PRIMARY KEY (`parameter`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

INSERT INTO `endpoints-modulesettings-discord` (`parameter`, `value`) VALUES
  ('username', ''),
  ('avatar-url', ''),
  ('tts', '0'),
  ('use-embeds', '1')
ON DUPLICATE KEY UPDATE `parameter` = `parameter`;
