package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
)

// Constants for file permissions and the YAML separator
const (
	dirPermissions  os.FileMode = 0750
	filePermissions os.FileMode = 0640
	yamlSeparator               = "---\n# Source: "
	bufferSize                  = 1048576 // 1MB buffer for scanner
)

var force bool // Flag to force deletion of existing output directory

func init() {
	flag.BoolVar(&force, "f", false, "Overwrite existing output directory")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: schelm [options] OUTPUT_DIR\n")
		flag.PrintDefaults()
	}
}

// scanYamlSpecs is a split function for bufio.Scanner to split input by the custom YAML separator.
func scanYamlSpecs(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	// Use the constant defined above
	separatorBytes := []byte(yamlSeparator)
	if i := bytes.Index(data, separatorBytes); i >= 0 {
		// We found a separator. Return the data before it.
		return i + len(separatorBytes), data[0:i], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

func splitSpec(token string) (string, string) {
	if i := strings.Index(token, "\n"); i >= 0 {
		return token[0:i], token[i+1:]
	}
	// If no newline is found, it's likely an incomplete token or the last part.
	// For simplicity, we'll assume valid input structure where source is always present.
	// A more robust implementation might handle malformed tokens differently.
	return token, "" // Return the whole token as source if no newline
}

// parseFlagsAndArgs parses command-line flags and arguments.
// It returns the output directory path or an error.
func parseFlagsAndArgs() (string, error) {
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		return "", fmt.Errorf("expected exactly one argument: OUTPUT_DIR")
	}
	outputDir := flag.Arg(0)
	if outputDir == "" {
		flag.Usage()
		return "", fmt.Errorf("output directory argument cannot be empty")
	}
	return outputDir, nil
}

// setupOutputDirectory ensures the output directory exists, creating or clearing it based on the force flag.
func setupOutputDirectory(outputDir string, force bool) error {
	stat, err := os.Stat(outputDir)
	if err == nil { // Directory exists
		if !stat.IsDir() {
			return fmt.Errorf(`"%s" exists but is not a directory`, outputDir)
		}
		if force {
			log.Printf("Removing existing output directory %s (-f specified)\n", outputDir)
			if err := os.RemoveAll(outputDir); err != nil {
				return fmt.Errorf("failed to remove existing directory %s: %w", outputDir, err)
			}
		} else {
			return fmt.Errorf(`output directory "%s" already exists. Use -f to overwrite`, outputDir)
		}
	} else if !os.IsNotExist(err) {
		// Another error occurred during Stat
		return fmt.Errorf("failed to check output directory %s: %w", outputDir, err)
	}

	// Directory doesn't exist (or was removed), create it.
	log.Printf("Creating output directory %s\n", outputDir)
	if err := os.MkdirAll(outputDir, dirPermissions); err != nil {
		return fmt.Errorf("failed to create output directory %s: %w", outputDir, err)
	}
	return nil
}

// writeOrAppendSpec writes content to a new file or appends it to an existing one.
func writeOrAppendSpec(outputDir, source, content string) error {
	destinationFile := path.Join(outputDir, source)
	dir := path.Dir(destinationFile)

	// Ensure the subdirectory for the file exists
	if err := os.MkdirAll(dir, dirPermissions); err != nil {
		return fmt.Errorf("error creating directory %s: %w", dir, err)
	}

	// Check if the file already exists
	if _, err := os.Stat(destinationFile); os.IsNotExist(err) {
		// File does not exist, create and write
		log.Printf("Creating %s", destinationFile)
		if err := os.WriteFile(destinationFile, []byte(content), filePermissions); err != nil {
			return fmt.Errorf("error writing new file %s: %w", destinationFile, err)
		}
	} else if err == nil {
		// File exists, append
		log.Printf("Appending to %s", destinationFile)
		f, openErr := os.OpenFile(destinationFile, os.O_APPEND|os.O_WRONLY, filePermissions)
		if openErr != nil {
			return fmt.Errorf("error opening file %s for appending: %w", destinationFile, openErr)
		}
		defer f.Close() // Ensure file is closed

		// Add separator before appending new content
		// Ensure there's exactly one newline before the standard YAML separator '---'
		// This assumes the previous content might or might not end with a newline.
		separator := "\n---\n"
		if !strings.HasSuffix(content, "\n") {
			separator = "\n" + separator // Add extra newline if content doesn't end with one
		}

		if _, writeErr := f.WriteString(separator + content); writeErr != nil {
			return fmt.Errorf("error appending to file %s: %w", destinationFile, writeErr)
		}
		// Check close error explicitly if needed, though defer handles the call.
		// if closeErr := f.Close(); closeErr != nil {
		// 	 return fmt.Errorf("error closing file %s after append: %w", destinationFile, closeErr)
		// }
	} else {
		// Another error occurred during Stat
		return fmt.Errorf("error checking file %s: %w", destinationFile, err)
	}
	return nil
}

// processInput reads from stdin, splits the content, and writes/appends specs.
func processInput(outputDir string) error {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(scanYamlSpecs)
	// Allow for tokens (specs) up to 1MB in size
	scanner.Buffer(make([]byte, bufio.MaxScanTokenSize), bufferSize)

	// Discard the first part of the stream (before the first separator)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading initial input: %w", err)
		}
		// Input might be empty or contain no separators, which could be valid?
		log.Println("Warning: Input stream is empty or contains no separators.")
		return nil
	}

	// Process the rest of the stream
	for scanner.Scan() {
		source, content := splitSpec(scanner.Text())
		if source == "" {
			log.Println("Warning: Skipping empty source path in input.")
			continue
		}
		if err := writeOrAppendSpec(outputDir, source, content); err != nil {
			// Log the specific error and continue processing other specs?
			// Or return immediately? Returning seems safer for a batch process.
			return fmt.Errorf("failed to process spec for source %s: %w", source, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning input stream: %w", err)
	}
	return nil
}

func main() {
	// 1. Parse flags and arguments
	outputDirectory, err := parseFlagsAndArgs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// 2. Setup output directory
	if err := setupOutputDirectory(outputDirectory, force); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// 3. Process the input stream
	if err := processInput(outputDirectory); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	log.Println("Processing complete.")
}
