import sys
from pathlib import Path

file_path = Path('d:/TEST/MangaTranslator/core/pipeline.py')
content = file_path.read_text(encoding='utf-8')

if 'RotatorSession' not in content:
    content = content.replace(
        'from core.image.image_utils import cv2_to_pil, pil_to_cv2, process_bubble_image_cached',
        'from core.image.image_utils import cv2_to_pil, pil_to_cv2, process_bubble_image_cached\nfrom core.rotator import RotatorSession, translate_with_retry'
    )

    # 1. Update _batch_translate_parallel signature
    content = content.replace(
        'cancellation_manager: Optional["CancellationManager"] = None,\n) -> Dict[str, Any]:',
        'cancellation_manager: Optional["CancellationManager"] = None,\n    rotator=None,\n) -> Dict[str, Any]:'
    )

    # 2. Update _process_single inside parallel
    content = content.replace(
'''    def _process_single(img_path: Path, index: int) -> Tuple[str, str]:
        """Run translate_and_render for a single image. Returns (display_path, error_key)."""
        output_path, display_path, error_key = _resolve_output_path(
            img_path, input_dir, output_dir, config, preserve_structure
        )
        log_message(
            f"Processing {index + 1}/{total_images}: {display_path}",
            always_print=True,
        )
        translate_and_render(
            img_path, config, output_path, cancellation_manager=cancellation_manager
        )
        return display_path, error_key''',
'''    def _process_single(img_path: Path, index: int, rotator_obj) -> Tuple[str, str, bool]:
        output_path, display_path, error_key = _resolve_output_path(
            img_path, input_dir, output_dir, config, preserve_structure
        )
        if output_path.exists():
            log_message(f"Skipping {index + 1}/{total_images}: {display_path} (Already exists)", always_print=True)
            return display_path, error_key, True
            
        log_message(f"Processing {index + 1}/{total_images}: {display_path}", always_print=True)
        translate_with_retry(img_path, config, output_path, rotator_obj, cancellation_manager=cancellation_manager, display_path=display_path)
        return display_path, error_key, False'''
    )

    # 3. Update _worker inside parallel
    content = content.replace(
        'async def _worker(img_path: Path, index: int, executor: ThreadPoolExecutor):',
        'async def _worker(img_path: Path, index: int, executor: ThreadPoolExecutor, rotator_obj):'
    )

    content = content.replace(
'''                await loop.run_in_executor(executor, _process_single, img_path, index)
                with results_lock:
                    results["success_count"] += 1
                    completed_count += 1
                    count = completed_count''',
'''                _, _, skipped = await loop.run_in_executor(executor, _process_single, img_path, index, rotator_obj)
                with results_lock:
                    results["success_count"] += 1
                    completed_count += 1
                    count = completed_count
                    if skipped:
                        results["skipped_count"] = results.get("skipped_count", 0) + 1'''
    )

    # 4. Update worker invocation
    content = content.replace(
        'tasks = [_worker(img, i, executor) for i, img in enumerate(remaining, start=1)]',
        'tasks = [_worker(img, i, executor, rotator) for i, img in enumerate(remaining, start=1)]'
    )

    # 5. Update batch_translate_images initialization
    content = content.replace(
'''    if progress_callback:
        progress_callback(0.0, f"Starting batch processing of {total_images} images...")

    if config.parallel_requests > 1:''',
'''    if progress_callback:
        progress_callback(0.0, f"Starting batch processing of {total_images} images...")

    rotator = RotatorSession(config)

    if config.parallel_requests > 1:'''
    )

    # 6. Update _batch_translate_parallel call in batch_translate_images
    content = content.replace(
'''                preserve_structure=preserve_structure,
                progress_callback=progress_callback,
                cancellation_manager=cancellation_manager,
            )
        )''',
'''                preserve_structure=preserve_structure,
                progress_callback=progress_callback,
                cancellation_manager=cancellation_manager,
                rotator=rotator,
            )
        )'''
    )

    # 7. Update sequential loop in batch_translate_images
    content = content.replace(
'''        for i, img_path in enumerate(image_files):
            try:
                output_path, display_path, error_key = _resolve_output_path(
                    img_path, input_dir, output_dir, config, preserve_structure
                )

                if cancellation_manager and cancellation_manager.is_cancelled():
                    raise CancellationError("Batch process cancelled by user.")

                if progress_callback:
                    current_progress = i / total_images
                    progress_callback(
                        current_progress,
                        f"Processing image {i + 1}/{total_images}: {display_path}",
                    )

                log_message(
                    f"Processing {i + 1}/{total_images}: {display_path}",
                    always_print=True,
                )

                translate_and_render(
                    img_path,
                    config,
                    output_path,
                    cancellation_manager=cancellation_manager,
                )

                results["success_count"] += 1''',
'''        for i, img_path in enumerate(image_files):
            try:
                output_path, display_path, error_key = _resolve_output_path(
                    img_path, input_dir, output_dir, config, preserve_structure
                )

                if cancellation_manager and cancellation_manager.is_cancelled():
                    raise CancellationError("Batch process cancelled by user.")

                if output_path.exists():
                    log_message(f"Skipping {i + 1}/{total_images}: {display_path} (Already exists)", always_print=True)
                    results["success_count"] += 1
                    results["skipped_count"] = results.get("skipped_count", 0) + 1
                    continue

                if progress_callback:
                    current_progress = i / total_images
                    progress_callback(
                        current_progress,
                        f"Processing image {i + 1}/{total_images}: {display_path}",
                    )

                log_message(
                    f"Processing {i + 1}/{total_images}: {display_path}",
                    always_print=True,
                )

                translate_with_retry(
                    img_path,
                    config,
                    output_path,
                    rotator,
                    cancellation_manager=cancellation_manager,
                    display_path=display_path
                )

                results["success_count"] += 1'''
    )

    # 8. Add retry loop at the end of batch_translate_images
    content = content.replace(
'''    if progress_callback:
        progress_callback(1.0, "Processing complete")''',
'''    # RETRY PASS
    if results.get("error_count", 0) > 0:
        log_message(f"\\n🔄 RETRY PASS: Attempting to re-translate {results['error_count']} failed images...", always_print=True)
        rotator.reset_exhaustion()
        
        failed_files = []
        for img_path in image_files:
            output_path, display_path, error_key = _resolve_output_path(img_path, input_dir, output_dir, config, preserve_structure)
            if not output_path.exists():
                failed_files.append((img_path, output_path, display_path, error_key))

        for i, (img_path, output_path, display_path, error_key) in enumerate(failed_files):
            if cancellation_manager and cancellation_manager.is_cancelled():
                break
            
            if progress_callback:
                progress_callback(i / len(failed_files), f"Retrying failed images... ({i + 1}/{len(failed_files)})")
                
            try:
                log_message(f"Retrying {i + 1}/{len(failed_files)}: {display_path}", always_print=True)
                translate_with_retry(img_path, config, output_path, rotator, cancellation_manager=cancellation_manager, display_path=display_path)
                
                results["success_count"] += 1
                results["error_count"] -= 1
                if error_key in results["errors"]:
                    del results["errors"][error_key]
                log_message(f"Retry successful for {display_path}", always_print=True)
            except Exception as e:
                log_message(f"Retry failed for {display_path}: {e}", always_print=True)
                results["errors"][error_key] = str(e)

    if progress_callback:
        progress_callback(1.0, "Processing complete")'''
    )

    file_path.write_text(content, encoding='utf-8')
    print("Patched core/pipeline.py successfully!")
else:
    print("Already patched.")
