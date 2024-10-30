package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	namespace               = "fpms"
	containerName           = "client-apiserver-canary"
	targetPort              = "9280"
	pushGateway             = "http://k8s-monitori-pushgate-fcae943c1e-e1a58b32cb8c6cce.elb.ap-southeast-1.amazonaws.com/metrics/job/client_tcp_new"
	maxConcurrentConnections = 100 // Set your desired concurrency level
	clusterName             = "fpms-prod" // Your EKS cluster name
	cacheTTL                = 5 * time.Minute // Increased token cache duration
)

var (
	podRegex   = regexp.MustCompile(`\bclient\b`)
	tokenCache *TokenResponse
	cacheMutex sync.Mutex
)

// TokenResponse represents the structure of the response from the AWS EKS get-token command.
type TokenResponse struct {
	Token  string `json:"token"`
	Expiry int64  `json:"expiry"`
}

// getToken retrieves the AWS EKS token for the specified cluster name, caching it for subsequent calls.
func getToken() (string, error) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	// Check if the token is cached and still valid
	if tokenCache != nil && time.Now().Unix() < tokenCache.Expiry {
		fmt.Println("Using cached token.")
		return tokenCache.Token, nil
	}

	fmt.Println("Fetching new token.")
	// If not cached or expired, get a new token
	cmd := exec.Command("aws", "eks", "get-token", "--cluster-name", clusterName, "--output", "json")
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	var response TokenResponse
	if err := json.Unmarshal(output, &response); err != nil {
		return "", err
	}

	// Cache the token and its expiry time
	tokenCache = &TokenResponse{
		Token:  response.Token,
		Expiry: time.Now().Unix() + int64(cacheTTL.Seconds()),
	}

	fmt.Printf("New token fetched and cached. Expiry: %v\n", time.Unix(tokenCache.Expiry, 0))
	return tokenCache.Token, nil
}

// Executes a kubectl command to get all client pods in Running state
func getPods() ([]string, error) {
	fmt.Println("Fetching running pods...")
	cmd := exec.Command("kubectl", "get", "pods", "-n", namespace, "--field-selector=status.phase=Running")
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var pods []string
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) > 0 && podRegex.MatchString(fields[0]) {
			pods = append(pods, fields[0])
		}
	}

	fmt.Printf("Running pods found: %v\n", pods)
	return pods, scanner.Err()
}

// Counts TCP connections to the specified port in the specified pod's container
func countTCPConnections(pod string, token string) (int, error) {
	// Prepare kubectl command with the required token
	cmd := exec.Command("kubectl", "exec", "-n", namespace, pod, "--", "sh", "-c", fmt.Sprintf(`
		if ! which netstat > /dev/null; then
			apt-get update > /dev/null && apt-get install -y net-tools > /dev/null
		fi
		netstat -tn | grep ESTABLISHED | grep ":%s " | wc -l`, targetPort))

	// Set KUBECONFIG to use the token for authentication
	cmd.Env = append(os.Environ(), fmt.Sprintf("KUBECONFIG=%s", token))

	fmt.Printf("Counting TCP connections in pod: %s\n", pod)
	out, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	countStr := strings.TrimSpace(string(out))
	count, err := strconv.Atoi(countStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse TCP connection count for pod %s: %v", pod, err)
	}

	fmt.Printf("TCP connection count for pod %s: %d\n", pod, count)
	return count, nil
}

// Sends the total TCP connection count to the Push Gateway
func sendToPushGateway(totalTCPConnections int) error {
	data := fmt.Sprintf("client_tcp_new %d\n", totalTCPConnections)
	fmt.Printf("Sending total TCP connections to Push Gateway: %d\n", totalTCPConnections)

	resp, err := http.Post(pushGateway, "text/plain", strings.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Push Gateway error: %s", string(body))
	}

	fmt.Println("Successfully sent to Push Gateway.")
	return nil
}

// Main execution with controlled concurrency using a worker pool
func main() {
	startTime := time.Now()
	fmt.Println("Starting TCP connection counting...")

	pods, err := getPods()
	if err != nil {
		fmt.Printf("Error fetching pods: %v\n", err)
		return
	}

	var totalTCPConnections int
	var mu sync.Mutex
	var wg sync.WaitGroup
	workers := make(chan struct{}, maxConcurrentConnections) // Create a worker pool

	// Fetch the token once and reuse it
	token, err := getToken()
	if err != nil {
		fmt.Printf("Error fetching token: %v\n", err)
		return
	}

	for _, pod := range pods {
		wg.Add(1)

		// Acquire a worker slot by sending an empty struct to the channel
		workers <- struct{}{}

		go func(p string) {
			defer wg.Done()
			defer func() { <-workers }() // Release the worker slot

			tcpCount, err := countTCPConnections(p, token) // Pass the token here
			if err != nil {
				fmt.Printf("Failed to get TCP count for pod %s: %v\n", p, err)
				return
			}

			mu.Lock()
			totalTCPConnections += tcpCount
			mu.Unlock()
		}(pod)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	fmt.Printf("Total TCP connections counted: %d\n", totalTCPConnections)
	fmt.Printf("Completed in: %v\n", time.Since(startTime))

	//if err := sendToPushGateway(totalTCPConnections); err != nil {
	//	fmt.Printf("Error sending to Push Gateway: %v\n", err)
	//} else {
	//	fmt.Println("Successfully sent to Push Gateway.")
	//}
}
