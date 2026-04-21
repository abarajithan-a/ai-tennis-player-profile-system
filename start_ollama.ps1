# start_ollama.ps1
taskkill /IM ollama.exe /F 2>$null
Start-Sleep -Seconds 2

# 1. Disable the iGPU. Force CPU-only execution.
$env:OLLAMA_VULKAN = "0"

# 2. Process exactly one prompt at a time to prevent RAM bandwidth splitting.
$env:OLLAMA_NUM_PARALLEL = "1"

# 3. Thread Pinning.Forcing 4 threads prevents Windows from offloading heavy math to the slower 
# 	E-cores.
$env:OLLAMA_NUM_THREAD = "4"

# 4. Flash Attention. This optimizes how the context window is mapped in memory, 
# speeding up prompt reading and saving a little RAM.
$env:OLLAMA_FLASH_ATTENTION = "1"

Start-Process ollama -ArgumentList "serve" -WindowStyle Hidden

Write-Host "Ollama started in background"