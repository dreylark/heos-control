package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dreylark/heos-control/internal/app"
	"github.com/dreylark/heos-control/internal/config"
	"github.com/dreylark/heos-control/internal/journal"
)

var version = "dev"
var commit = "unknown"

func main() { os.Exit(run()) }

func run() int {
	var level slog.LevelVar
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: &level}))
	args := os.Args[1:]
	if len(args) == 1 && args[0] == "version" {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"version": version, "commit": commit})
		return 0
	}
	if len(args) > 0 && (args[0] == "config" || args[0] == "doctor") {
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()
		return runDiagnostic(ctx, args, os.Stdout, os.Stderr)
	}
	command := "serve"
	if len(args) > 0 && (args[0] == "migrate" || args[0] == "serve") {
		command, args = args[0], args[1:]
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	path := flags.String("config", "/etc/heos-control/config.json", "JSON configuration file")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		logger.Error("unexpected arguments")
		return 2
	}
	cfg, err := config.Load(*path)
	if err != nil {
		logger.Error("configuration rejected", "error", err)
		return 1
	}
	var configuredLevel slog.Level
	if cfg.LogLevel != "" {
		_ = configuredLevel.UnmarshalText([]byte(cfg.LogLevel))
	} // Validated by config.Load.
	level.Set(configuredLevel)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	logger.Info("starting", "command", command, "version", version, "commit", commit)
	if command == "migrate" {
		err = journal.Migrate(ctx, cfg.Database)
	} else {
		err = app.Run(ctx, cfg, logger)
	}
	if err != nil {
		logger.Error("command failed", "error", err)
		return 1
	}
	if command == "migrate" {
		logger.Info("schema up to date")
	}
	return 0
}
