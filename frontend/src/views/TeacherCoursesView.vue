<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  Plus,
  Refresh,
  Edit,
  Delete,
  Upload,
  Download,
  Collection,
  Memo,
  Files,
  DocumentAdd,
  CircleCheck,
  CircleClose,
  Warning,
  Folder,
  FolderOpened,
  Document,
  UploadFilled
} from '@element-plus/icons-vue'
import api from '../api/client'

const roles = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) ? u.roles : []
  } catch {
    return []
  }
})
const isTeacher = computed(() => roles.value.includes('teacher'))

const loadingCourses = ref(false)
const courses = ref([])
const courseId = ref(null)

const loadingChapters = ref(false)
const chapters = ref([])
const chapterId = ref(null)

const loadingSections = ref(false)
const sections = ref([]) // 用于“课程结构管理”左侧列表（当前章节的小节）
const allSections = ref([]) // 用于所有下拉框（课程下所有小节）
const sectionId = ref(null)

const loadingKps = ref(false)
const knowledgePoints = ref([])

const chapterOpen = ref(false)
const chapterSubmitting = ref(false)
const chapterForm = ref({ id: null, name: '' })

const sectionOpen = ref(false)
const sectionSubmitting = ref(false)
const sectionForm = ref({ id: null, name: '' })

const kpOpen = ref(false)
const kpSubmitting = ref(false)
const kpForm = ref({ id: null, name: '', chapter_id: null, section_id: null })

const resourceStatus = ref('all')
const loadingResources = ref(false)
const resources = ref([])

const uploadSubmitting = ref(false)
const uploadForm = ref({ title: '', description: '', chapter_id: null, section_id: null, knowledge_point_id: null, file: null })

const batchFiles = ref([])
const batchSubmitting = ref(false)
const batchResults = ref([])
const batchChapterId = ref(null)
const batchSectionId = ref(null)

const catalogFile = ref(null)
const catalogSubmitting = ref(false)

const kpSections = computed(() => {
  if (!kpForm.value.chapter_id) return []
  return allSections.value.filter(s => String(s.chapter_id) === String(kpForm.value.chapter_id))
})

const selectedChapterName = computed(() => {
  if (!kpForm.value.chapter_id) return null
  const ch = chapters.value.find(c => c.id === kpForm.value.chapter_id)
  return ch ? ch.name : null
})

const selectedSectionName = computed(() => {
  if (!kpForm.value.section_id) return null
  const s = allSections.value.find(sec => sec.id === kpForm.value.section_id)
  return s ? s.name : null
})

const currentChapterName = computed(() => {
  if (!isNumberValue(chapterId.value)) return null
  const ch = chapters.value.find(c => c.id === chapterId.value)
  return ch ? ch.name : null
})

const currentSectionName = computed(() => {
  if (!isNumberValue(sectionId.value)) return null
  const s = allSections.value.find(sec => sec.id === sectionId.value)
  return s ? s.name : null
})

const batchSections = computed(() => {
  if (!batchChapterId.value) return []
  return allSections.value.filter(s => String(s.chapter_id) === String(batchChapterId.value))
})

const uploadSections = computed(() => {
  if (!uploadForm.value.chapter_id) return []
  return allSections.value.filter(s => String(s.chapter_id) === String(uploadForm.value.chapter_id))
})

const uploadKpOptions = computed(() => {
  if (!courseId.value) return []
  let filtered = knowledgePoints.value.filter(k => String(k.course_id) === String(courseId.value))
  if (uploadForm.value.section_id) {
    filtered = filtered.filter(k => String(k.section_id) === String(uploadForm.value.section_id))
  } else if (uploadForm.value.chapter_id) {
    filtered = filtered.filter(k => String(k.chapter_id) === String(uploadForm.value.chapter_id) && !k.section_id)
  } else {
    filtered = filtered.filter(k => !k.chapter_id && !k.section_id)
  }
  return filtered
})

const displayKnowledgePoints = computed(() => {
  if (!isNumberValue(courseId.value)) return []
  
  if (isNumberValue(sectionId.value)) {
    return knowledgePoints.value.filter(k => k.section_id === sectionId.value)
  }
  
  if (isNumberValue(chapterId.value)) {
    return knowledgePoints.value.filter(k => k.chapter_id === chapterId.value && !k.section_id)
  }
  
  return knowledgePoints.value.filter(k => !k.chapter_id && !k.section_id)
})

function onBatchFileChange(ev) {
  const files = ev?.target?.files
  if (files) {
    batchFiles.value = Array.from(files)
  }
}

function onCatalogFileChange(ev) {
  const f = ev?.target?.files?.[0]
  catalogFile.value = f || null
}

async function submitBatchUpload() {
  if (!isTeacher.value) {
    ElMessage.error('当前角色无上传权限')
    return
  }
  if (!isNumberValue(courseId.value)) {
    ElMessage.error('请先选择课程')
    return
  }
  if (batchFiles.value.length === 0) {
    ElMessage.warning('请选择要上传的文件')
    return
  }

  const fd = new FormData()
  batchFiles.value.forEach(f => fd.append('files', f))
  fd.append('course_id', String(courseId.value))
  if (isNumberValue(batchChapterId.value)) {
    fd.append('chapter_id', String(batchChapterId.value))
  }
  if (isNumberValue(batchSectionId.value)) {
    fd.append('section_id', String(batchSectionId.value))
  }

  batchSubmitting.value = true
  try {
    const resp = await api.post('/api/resources/batch-upload', fd, {
      headers: { 'Content-Type': 'multipart/form-data' }
    })
    batchResults.value = resp.data.results || []
    ElMessage.success(`批量处理完成：成功 ${batchResults.value.filter(r => r.status === 'success').length} 份`)
    batchFiles.value = []
    const fileInput = document.querySelector('.batch-hidden-input')
    if (fileInput) fileInput.value = ''
    await fetchMyResources()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '批量上传失败')
  } finally {
    batchSubmitting.value = false
  }
}

async function submitCatalogImport() {
  if (!isTeacher.value) {
    ElMessage.error('当前角色无上传权限')
    return
  }
  if (!isNumberValue(courseId.value)) {
    ElMessage.error('请先选择课程')
    return
  }
  if (!catalogFile.value) {
    ElMessage.warning('请选择目录文件')
    return
  }

  const fd = new FormData()
  fd.append('file', catalogFile.value)
  fd.append('course_id', String(courseId.value))

  catalogSubmitting.value = true
  try {
    const resp = await api.post('/api/catalog/import', fd, {
      headers: { 'Content-Type': 'multipart/form-data' }
    })
    ElMessage.success(`目录导入成功：创建了 ${resp.data.chapters_count} 个章节和 ${resp.data.sections_count} 个小节`)
    catalogFile.value = null
    const fileInput = document.querySelector('.catalog-hidden-input')
    if (fileInput) fileInput.value = ''
    await Promise.all([fetchChapters(), fetchSections()])
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '目录导入失败')
  } finally {
    catalogSubmitting.value = false
  }
}

function isNumberValue(v) {
  return typeof v === 'number' && Number.isFinite(v)
}

async function fetchCourses() {
  loadingCourses.value = true
  try {
    const resp = await api.get('/api/me/courses')
    courses.value = resp.data.items || []
    if (!isNumberValue(courseId.value) && (courses.value || []).length > 0) {
      courseId.value = courses.value[0].id
    }
  } catch (e) {
    courses.value = []
  } finally {
    loadingCourses.value = false
  }
}

async function fetchChapters() {
  if (!isNumberValue(courseId.value)) {
    chapters.value = []
    chapterId.value = null
    return
  }
  loadingChapters.value = true
  try {
    const resp = await api.get('/api/chapters', { params: { course_id: courseId.value } })
    chapters.value = resp.data.items || []
  } catch (e) {
    chapters.value = []
  } finally {
    loadingChapters.value = false
  }
}

async function fetchSections() {
  if (!isNumberValue(chapterId.value)) {
    sections.value = []
    sectionId.value = null
    return
  }
  loadingSections.value = true
  try {
    const resp = await api.get('/api/sections', { params: { chapter_id: chapterId.value } })
    sections.value = resp.data.items || []
  } catch (e) {
    sections.value = []
  } finally {
    loadingSections.value = false
  }
}

async function fetchSectionsForKp() {
  if (!isNumberValue(courseId.value)) {
    allSections.value = []
    return
  }
  try {
    const resp = await api.get('/api/sections', { params: { course_id: courseId.value } })
    allSections.value = resp.data.items || []
  } catch (e) {
    allSections.value = []
  }
}

async function fetchKnowledgePoints() {
  if (!isNumberValue(courseId.value)) {
    knowledgePoints.value = []
    return
  }
  loadingKps.value = true
  try {
    const resp = await api.get('/api/knowledge-points', { params: { course_id: courseId.value } })
    knowledgePoints.value = resp.data.items || []
  } catch (e) {
    knowledgePoints.value = []
  } finally {
    loadingKps.value = false
  }
}

function openCreateChapter() {
  chapterForm.value = { id: null, name: '' }
  chapterOpen.value = true
}

function openEditChapter(row) {
  chapterForm.value = { id: row.id, name: row.name || '' }
  chapterOpen.value = true
}

async function submitChapter() {
  const name = (chapterForm.value.name || '').trim()
  if (!name) {
    ElMessage.warning('请输入章节名称')
    return
  }
  if (!isNumberValue(courseId.value)) {
    ElMessage.error('请先选择课程')
    return
  }
  chapterSubmitting.value = true
  try {
    if (chapterForm.value.id) {
      await api.patch(`/api/chapters/${chapterForm.value.id}`, { name })
      ElMessage.success('章节更新成功')
    } else {
      await api.post('/api/chapters', { name, course_id: courseId.value })
      ElMessage.success('新增章节成功')
    }
    chapterOpen.value = false
    await Promise.all([fetchChapters(), fetchSectionsForKp()])
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '操作失败')
  } finally {
    chapterSubmitting.value = false
  }
}

async function removeChapter(row) {
  try {
    await ElMessageBox.confirm(`确认删除章节「${row.name}」吗？删除将同时删除该章节下的所有小节、知识点以及关联的资源文件。`, '级联删除确认', {
      type: 'warning',
      confirmButtonText: '确定删除',
      cancelButtonText: '取消',
      confirmButtonClass: 'el-button--danger'
    })
  } catch {
    return
  }
  try {
    await api.delete(`/api/chapters/${row.id}`)
    ElMessage.success('章节已删除')
    await Promise.all([fetchChapters(), fetchSectionsForKp(), fetchKnowledgePoints()])
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '删除失败')
  }
}

function openCreateSection() {
  sectionForm.value = { id: null, name: '' }
  sectionOpen.value = true
}

function openEditSection(row) {
  sectionForm.value = { id: row.id, name: row.name || '' }
  sectionOpen.value = true
}

async function submitSection() {
  const name = (sectionForm.value.name || '').trim()
  if (!name) {
    ElMessage.warning('请输入小节名称')
    return
  }
  if (!isNumberValue(chapterId.value)) {
    ElMessage.error('请先选择章节')
    return
  }
  sectionSubmitting.value = true
  try {
    if (sectionForm.value.id) {
      await api.patch(`/api/sections/${sectionForm.value.id}`, { name })
      ElMessage.success('小节更新成功')
    } else {
      await api.post('/api/sections', { name, chapter_id: chapterId.value })
      ElMessage.success('新增小节成功')
    }
    sectionOpen.value = false
    await Promise.all([fetchSections(), fetchSectionsForKp()])
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '操作失败')
  } finally {
    sectionSubmitting.value = false
  }
}

async function removeSection(row) {
  try {
    await ElMessageBox.confirm(`确认删除小节「${row.name}」吗？删除将同时删除该小节下的所有知识点以及关联的资源文件。`, '级联删除确认', {
      type: 'warning',
      confirmButtonText: '确定删除',
      cancelButtonText: '取消',
      confirmButtonClass: 'el-button--danger'
    })
  } catch {
    return
  }
  try {
    await api.delete(`/api/sections/${row.id}`)
    ElMessage.success('小节已删除')
    await Promise.all([fetchSections(), fetchSectionsForKp(), fetchKnowledgePoints()])
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '删除失败')
  }
}

function openCreateKp() {
  kpForm.value = { id: null, name: '', chapter_id: chapterId.value, section_id: sectionId.value }
  kpOpen.value = true
}

function openEditKp(row) {
  kpForm.value = { id: row.id, name: row.name || '', chapter_id: row.chapter_id, section_id: row.section_id }
  kpOpen.value = true
}

async function submitKp() {
  const name = (kpForm.value.name || '').trim()
  if (!name) {
    ElMessage.warning('请输入知识点名称')
    return
  }
  if (!isNumberValue(courseId.value)) {
    ElMessage.error('请先选择课程')
    return
  }
  kpSubmitting.value = true
  const payload = { name, course_id: courseId.value }
  if (isNumberValue(kpForm.value.chapter_id)) {
    payload.chapter_id = kpForm.value.chapter_id
  }
  if (isNumberValue(kpForm.value.section_id)) {
    payload.section_id = kpForm.value.section_id
  }
  try {
    if (kpForm.value.id) {
      await api.patch(`/api/knowledge-points/${kpForm.value.id}`, { 
        name,
        chapter_id: payload.chapter_id,
        section_id: payload.section_id
      })
      ElMessage.success('知识点更新成功')
    } else {
      await api.post('/api/knowledge-points', payload)
      ElMessage.success('新增知识点成功')
    }
    kpOpen.value = false
    await fetchKnowledgePoints()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '操作失败')
  } finally {
    kpSubmitting.value = false
  }
}

async function removeKp(row) {
  try {
    await ElMessageBox.confirm(`确认删除知识点「${row.name}」吗？`, '删除确认', {
      type: 'warning',
      confirmButtonText: '确定',
      cancelButtonText: '取消'
    })
  } catch {
    return
  }
  try {
    await api.delete(`/api/knowledge-points/${row.id}`)
    ElMessage.success('知识点已删除')
    await fetchKnowledgePoints()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '删除失败')
  }
}

async function fetchMyResources() {
  if (!isNumberValue(courseId.value)) {
    resources.value = []
    return
  }
  loadingResources.value = true
  try {
    const resp = await api.get('/api/me/resources', {
      params: {
        status: resourceStatus.value || 'all',
        course_id: courseId.value,
      },
    })
    resources.value = resp.data.items || []
  } catch (e) {
    resources.value = []
    ElMessage.error(e?.response?.data?.error?.message || '加载资源失败')
  } finally {
    loadingResources.value = false
  }
}

function onFileChange(ev) {
  const f = ev?.target?.files?.[0]
  uploadForm.value.file = f || null
}

async function submitUpload() {
  if (!isTeacher.value) {
    ElMessage.error('当前角色无上传权限')
    return
  }
  if (!isNumberValue(courseId.value)) {
    ElMessage.error('请先选择课程')
    return
  }
  if (!uploadForm.value.file) {
    ElMessage.warning('请选择要上传的文件')
    return
  }
  const fd = new FormData()
  fd.append('file', uploadForm.value.file)
  fd.append('title', (uploadForm.value.title || '').trim() || uploadForm.value.file.name)
  if ((uploadForm.value.description || '').trim()) fd.append('description', uploadForm.value.description.trim())
  fd.append('course_id', String(courseId.value))
  if (isNumberValue(uploadForm.value.chapter_id)) {
    fd.append('chapter_id', String(uploadForm.value.chapter_id))
  }
  if (isNumberValue(uploadForm.value.section_id)) {
    fd.append('section_id', String(uploadForm.value.section_id))
  }
  if (isNumberValue(uploadForm.value.knowledge_point_id)) {
    fd.append('knowledge_point_id', String(uploadForm.value.knowledge_point_id))
  }
  uploadSubmitting.value = true
  try {
    await api.post('/api/resources/upload', fd, { headers: { 'Content-Type': 'multipart/form-data' } })
    ElMessage.success('资源上传成功，请等待管理员审核')
    uploadForm.value = { title: '', description: '', chapter_id: null, section_id: null, knowledge_point_id: null, file: null }
    const fileInput = document.querySelector('input[type="file"]')
    if (fileInput) fileInput.value = ''
    await fetchMyResources()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '上传失败')
  } finally {
    uploadSubmitting.value = false
  }
}

async function removeResource(row) {
  try {
    await ElMessageBox.confirm(`确认删除资源「${row.title}」吗？`, '删除确认', { type: 'warning' })
  } catch {
    return
  }
  try {
    await api.delete(`/api/resources/${row.id}`)
    ElMessage.success('资源已删除')
    await fetchMyResources()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '删除失败')
  }
}

async function download(item) {
  try {
    const token = localStorage.getItem('token')
    const baseUrl = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:5000'
    const url = `${baseUrl}/api/resources/${item.id}/download?token=${token}`
    window.open(url, '_blank')
  } catch (e) {
    ElMessage.error('下载失败')
  }
}

function getStatusType(status) {
  switch (status) {
    case 'approved': return 'success'
    case 'pending': return 'warning'
    case 'rejected': return 'danger'
    default: return 'info'
  }
}

function getStatusLabel(status) {
  switch (status) {
    case 'approved': return '已通过'
    case 'pending': return '待审核'
    case 'rejected': return '已拒绝'
    default: return status
  }
}

watch(courseId, async () => {
  chapterId.value = null
  sectionId.value = null
  uploadForm.value = { title: '', description: '', chapter_id: null, section_id: null, knowledge_point_id: null, file: null }
  batchChapterId.value = null
  batchSectionId.value = null
  kpForm.value.chapter_id = null
  kpForm.value.section_id = null
  await Promise.all([fetchChapters(), fetchSectionsForKp(), fetchKnowledgePoints(), fetchMyResources()])
})

watch(chapterId, async () => {
  sectionId.value = null
  uploadForm.value.section_id = null
  batchSectionId.value = null
  await fetchSections()
})

watch(sectionId, async () => {
  uploadForm.value.section_id = sectionId.value
  uploadForm.value.knowledge_point_id = null
})

watch(batchChapterId, async () => {
  batchSectionId.value = null
})

watch(resourceStatus, fetchMyResources)

onMounted(async () => {
  await fetchCourses()
  await Promise.all([fetchChapters(), fetchSectionsForKp(), fetchKnowledgePoints(), fetchMyResources()])
})
</script>

<template>
  <div class="teacher-courses-container">
    <el-row :gutter="24">
      <el-col :xs="24" :md="6">
        <el-card class="course-list-card" shadow="never">
          <template #header>
            <div class="card-header">
              <div class="title-with-icon">
                <el-icon><Collection /></el-icon>
                <span>我的课程</span>
              </div>
              <el-button circle :icon="Refresh" size="small" :loading="loadingCourses" @click="fetchCourses" />
            </div>
          </template>

          <div v-if="!isTeacher" class="empty-state">
            <el-icon :size="40" color="#909399"><Warning /></el-icon>
            <p>需要教师账号权限</p>
          </div>
          <div v-else-if="(courses || []).length === 0" class="empty-state">
            <el-icon :size="40" color="#909399"><Files /></el-icon>
            <p>暂无分配课程</p>
          </div>
          <div v-else class="course-selector">
            <div
              v-for="c in courses"
              :key="c.id"
              class="course-item"
              :class="{ 'active': courseId === c.id }"
              @click="courseId = c.id"
            >
              <span class="course-name">{{ c.name }}</span>
              <el-icon v-if="courseId === c.id"><CircleCheck /></el-icon>
            </div>
          </div>
        </el-card>
      </el-col>

      <el-col :xs="24" :md="18">
        <el-tabs type="border-card" class="management-tabs">
          <el-tab-pane>
            <template #label>
              <div class="tab-label">
                <el-icon><FolderOpened /></el-icon>课程结构管理
              </div>
            </template>

            <el-row :gutter="16">
              <el-col :span="8">
                <el-card shadow="never" class="structure-card">
                  <template #header>
                    <div class="card-header">
                      <span class="card-title">章节</span>
                      <el-button type="primary" size="small" :icon="Plus" :disabled="!isNumberValue(courseId)" @click="openCreateChapter">新增</el-button>
                    </div>
                  </template>
                  <div v-loading="loadingChapters" class="structure-list">
                    <div v-if="chapters.length === 0" class="empty-hint">暂无章节</div>
                    <div
                      v-for="ch in chapters"
                      :key="ch.id"
                      class="structure-item"
                      :class="{ 'active': chapterId === ch.id }"
                      @click="chapterId = (chapterId === ch.id ? null : ch.id)"
                      :title="ch.name"
                    >
                      <el-icon><Folder /></el-icon>
                      <span class="item-name">{{ ch.name }}</span>
                      <div class="item-actions">
                        <el-button size="small" :icon="Edit" link @click.stop="openEditChapter(ch)"></el-button>
                        <el-button size="small" type="danger" :icon="Delete" link @click.stop="removeChapter(ch)"></el-button>
                      </div>
                    </div>
                  </div>
                </el-card>
              </el-col>

              <el-col :span="8">
                <el-card shadow="never" class="structure-card">
                  <template #header>
                    <div class="card-header">
                      <span class="card-title">小节</span>
                      <el-button type="primary" size="small" :icon="Plus" :disabled="!isNumberValue(chapterId)" @click="openCreateSection">新增</el-button>
                    </div>
                  </template>
                  <div v-loading="loadingSections" class="structure-list">
                    <div v-if="!isNumberValue(chapterId)" class="empty-hint">请先选择章节</div>
                    <div v-else-if="sections.length === 0" class="empty-hint">暂无小节</div>
                    <div
                      v-for="s in sections"
                      :key="s.id"
                      class="structure-item"
                      :class="{ 'active': sectionId === s.id }"
                      @click="sectionId = (sectionId === s.id ? null : s.id)"
                      :title="s.name"
                    >
                      <el-icon><Document /></el-icon>
                      <span class="item-name">{{ s.name }}</span>
                      <div class="item-actions">
                        <el-button size="small" :icon="Edit" link @click.stop="openEditSection(s)"></el-button>
                        <el-button size="small" type="danger" :icon="Delete" link @click.stop="removeSection(s)"></el-button>
                      </div>
                    </div>
                  </div>
                </el-card>
              </el-col>

              <el-col :span="8">
                <el-card shadow="never" class="structure-card">
                  <template #header>
                    <div class="card-header">
                      <span class="card-title">知识点</span>
                      <el-button type="primary" size="small" :icon="Plus" :disabled="!isNumberValue(courseId)" @click="openCreateKp">新增</el-button>
                    </div>
                  </template>
                  <div v-loading="loadingKps" class="structure-list">
                    <div v-if="displayKnowledgePoints.length === 0" class="empty-hint">
                      {{ currentSectionName ? '该小节暂无直属知识点' : currentChapterName ? '该章节暂无直属知识点' : '该课程暂无直属知识点' }}
                    </div>
                    <div
                      v-for="k in displayKnowledgePoints"
                      :key="k.id"
                      class="structure-item kp-item"
                      :title="k.name"
                    >
                      <el-icon><Memo /></el-icon>
                      <span class="item-name">{{ k.name }}</span>
                      <div class="item-actions">
                        <el-button size="small" :icon="Edit" link @click.stop="openEditKp(k)"></el-button>
                        <el-button size="small" type="danger" :icon="Delete" link @click.stop="removeKp(k)"></el-button>
                      </div>
                    </div>
                  </div>
                </el-card>
              </el-col>
            </el-row>

            <el-divider />

            <el-card shadow="never">
              <template #header>
                <div class="card-header">
                  <span class="card-title">导入课程目录</span>
                </div>
              </template>
              <el-alert type="info" :closable="false" show-icon>
                <template #title>
                  <p style="margin: 0">支持 JSON 或 TXT 格式文件。</p>
                  <p style="margin: 4px 0 0 0">
                    <strong>JSON 示例：</strong><code>[{"name":"第一章","sections":[{"name":"1.1"}]}]</code>
                  </p>
                  <p style="margin: 4px 0 0 0">
                    <strong>TXT 示例：</strong>使用 <code>#</code> 表示章节，<code>##</code> 表示小节。例如：<br/>
                    <code># 第一章 概述</code><br/>
                    <code>## 1.1 课程背景</code>
                  </p>
                </template>
              </el-alert>
              <div class="catalog-import">
                <div class="file-input-wrapper">
                  <el-button type="primary" plain :icon="UploadFilled">选择目录文件</el-button>
                  <input type="file" accept=".txt,.json" :disabled="catalogSubmitting" @change="onCatalogFileChange" class="hidden-input catalog-hidden-input" />
                  <span v-if="catalogFile" class="file-name">{{ catalogFile.name }}</span>
                  <span v-else class="no-file">支持 .txt 或 .json 格式</span>
                </div>
                <el-button type="success" :loading="catalogSubmitting" @click="submitCatalogImport" :disabled="!catalogFile">
                  导入目录
                </el-button>
              </div>
            </el-card>
          </el-tab-pane>

          <el-tab-pane>
            <template #label>
              <div class="tab-label">
                <el-icon><Files /></el-icon>资源管理
              </div>
            </template>

            <el-tabs type="card" class="resource-sub-tabs">
              <el-tab-pane label="单份上传">
                <div class="upload-section">
                  <div class="section-header">
                    <el-icon><DocumentAdd /></el-icon>
                    <span>上传单份资源</span>
                  </div>

                  <el-form :model="uploadForm" label-position="top" class="upload-form">
                    <el-row :gutter="20">
                      <el-col :span="12">
                        <el-form-item label="资源标题">
                          <el-input v-model="uploadForm.title" placeholder="默认使用文件名" />
                        </el-form-item>
                      </el-col>
                      <el-col :span="12">
                        <el-form-item label="资源描述">
                          <el-input v-model="uploadForm.description" type="textarea" :rows="2" placeholder="简要描述资源内容..." />
                        </el-form-item>
                      </el-col>
                    </el-row>
                    <el-row :gutter="20">
                      <el-col :span="8">
                        <el-form-item label="关联章节（可选）">
                          <el-select v-model="uploadForm.chapter_id" filterable clearable placeholder="不选则关联课程" style="width: 100%" @change="uploadForm.section_id = null; uploadForm.knowledge_point_id = null">
                            <el-option v-for="ch in chapters" :key="ch.id" :label="ch.name" :value="ch.id" />
                          </el-select>
                        </el-form-item>
                      </el-col>
                      <el-col :span="8">
                        <el-form-item label="关联小节（可选）">
                          <el-select v-model="uploadForm.section_id" filterable clearable placeholder="不选则关联章节" style="width: 100%" @change="uploadForm.knowledge_point_id = null">
                            <el-option v-for="s in uploadSections" :key="s.id" :label="s.name" :value="s.id" />
                          </el-select>
                        </el-form-item>
                      </el-col>
                      <el-col :span="8">
                        <el-form-item label="关联知识点（可选）">
                          <el-select v-model="uploadForm.knowledge_point_id" filterable clearable placeholder="根据上方选择自动筛选" style="width: 100%">
                            <el-option v-for="k in uploadKpOptions" :key="k.id" :label="k.name" :value="k.id" />
                          </el-select>
                        </el-form-item>
                      </el-col>
                    </el-row>
                    <div class="upload-footer">
                      <div class="file-input-wrapper">
                        <el-button type="info" plain :icon="Upload">选择文件</el-button>
                        <input type="file" accept=".pdf,.doc,.docx,.pptx,.xlsx,.txt" :disabled="uploadSubmitting" @change="onFileChange" class="hidden-input" />
                        <span v-if="uploadForm.file" class="file-name">{{ uploadForm.file.name }}</span>
                        <span v-else class="no-file">未选择文件 (支持 PDF, Word, PPT, Excel, TXT)</span>
                      </div>
                      <el-button type="success" :loading="uploadSubmitting" @click="submitUpload" :disabled="!uploadForm.file">
                        开始上传
                      </el-button>
                    </div>
                  </el-form>
                </div>
              </el-tab-pane>

              <el-tab-pane label="批量上传">
                <div class="upload-section batch-upload">
                  <div class="section-header">
                    <el-icon><Files /></el-icon>
                    <span>批量上传资源</span>
                  </div>

                  <el-alert type="info" :closable="false" show-icon>
                    <template #title>
                      您可以选择将资源关联到特定章节或小节。若不选择，资源将直接关联到课程。
                    </template>
                  </el-alert>

                  <el-form label-position="top" style="margin-top: 20px">
                    <el-row :gutter="20">
                      <el-col :span="12">
                        <el-form-item label="选择章节（可选）">
                          <el-select v-model="batchChapterId" filterable clearable placeholder="不选则关联课程" style="width: 100%">
                            <el-option v-for="ch in chapters" :key="ch.id" :label="ch.name" :value="ch.id" />
                          </el-select>
                        </el-form-item>
                      </el-col>
                      <el-col :span="12">
                        <el-form-item label="选择小节（可选）">
                          <el-select v-model="batchSectionId" filterable clearable placeholder="不选则关联章节" style="width: 100%">
                            <el-option v-for="s in batchSections" :key="s.id" :label="s.name" :value="s.id" />
                          </el-select>
                        </el-form-item>
                      </el-col>
                    </el-row>
                  </el-form>

                  <div class="upload-footer" style="margin-top: 20px">
                    <div class="file-input-wrapper">
                      <el-button type="primary" plain :icon="Upload">多选文件</el-button>
                      <input type="file" multiple accept=".pdf,.doc,.docx,.pptx,.xlsx,.txt" :disabled="batchSubmitting" @change="onBatchFileChange" class="hidden-input batch-hidden-input" />
                      <span v-if="batchFiles.length > 0" class="file-name">已选择 {{ batchFiles.length }} 份文件</span>
                      <span v-else class="no-file">支持多选 PDF, Word, PPT, Excel, TXT 文件</span>
                    </div>
                    <el-button type="success" :loading="batchSubmitting" @click="submitBatchUpload" :disabled="batchFiles.length === 0">
                      一键处理并上传
                    </el-button>
                  </div>

                  <div v-if="batchResults.length > 0" class="batch-results">
                    <el-divider>处理结果</el-divider>
                    <el-table :data="batchResults" size="small" border stripe>
                      <el-table-column prop="filename" label="文件名" />
                      <el-table-column prop="chapter" label="章节" />
                      <el-table-column prop="section" label="小节" />
                      <el-table-column prop="kp" label="识别到的知识点">
                        <template #default="{ row }">
                          <el-tag size="small" type="success">{{ row.kp || '-' }}</el-tag>
                        </template>
                      </el-table-column>
                      <el-table-column prop="status" label="状态" width="100">
                        <template #default="{ row }">
                          <el-icon v-if="row.status === 'success'" color="#67C23A"><CircleCheck /></el-icon>
                          <el-icon v-else color="#F56C6C"><CircleClose /></el-icon>
                        </template>
                      </el-table-column>
                    </el-table>
                  </div>
                </div>
              </el-tab-pane>
            </el-tabs>

            <el-divider />

            <div class="tab-toolbar">
              <span class="tab-title">已上传资源</span>
              <div class="actions">
                <el-select v-model="resourceStatus" style="width: 140px" size="small">
                  <el-option label="全部状态" value="all" />
                  <el-option label="待审核" value="pending" />
                  <el-option label="已通过" value="approved" />
                  <el-option label="已拒绝" value="rejected" />
                </el-select>
                <el-button size="small" :icon="Refresh" :loading="loadingResources" @click="fetchMyResources" />
              </div>
            </div>

            <el-table :data="resources" v-loading="loadingResources" border stripe style="width: 100%">
              <el-table-column prop="title" label="资源标题" min-width="200">
                <template #default="{ row }">
                  <div class="res-cell">
                    <el-icon><Files /></el-icon>
                    <span>{{ row.title }}</span>
                  </div>
                </template>
              </el-table-column>
              <el-table-column prop="chapter" label="章节" width="120">
                <template #default="{ row }">
                  {{ row.chapter || '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="section" label="小节" width="120">
                <template #default="{ row }">
                  {{ row.section || '-' }}
                </template>
              </el-table-column>
              <el-table-column prop="knowledge_point" label="知识点" width="140">
                <template #default="{ row }">
                  {{ row.knowledge_point || '-' }}
                </template>
              </el-table-column>
              <el-table-column label="状态" width="110" align="center">
                <template #default="{ row }">
                  <el-tag :type="getStatusType(row.status)" size="small">
                    {{ getStatusLabel(row.status) }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column label="操作" width="120" fixed="right" align="center">
                <template #default="{ row }">
                  <el-button-group>
                    <el-button size="small" :icon="Download" @click="download(row)"></el-button>
                    <el-button size="small" type="danger" :icon="Delete" @click="removeResource(row)"></el-button>
                  </el-button-group>
                </template>
              </el-table-column>
            </el-table>
          </el-tab-pane>
        </el-tabs>
      </el-col>
    </el-row>

    <el-dialog v-model="chapterOpen" :title="chapterForm.id ? '编辑章节' : '新增章节'" width="420px" border-radius="12px">
      <el-form :model="chapterForm" label-position="top">
        <el-form-item label="章节名称" required>
          <el-input v-model="chapterForm.name" placeholder="如: 第一章 概述" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="chapterOpen = false">取消</el-button>
        <el-button type="primary" :loading="chapterSubmitting" @click="submitChapter">保存</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="sectionOpen" :title="sectionForm.id ? '编辑小节' : '新增小节'" width="420px" border-radius="12px">
      <el-form :model="sectionForm" label-position="top">
        <el-form-item label="小节名称" required>
          <el-input v-model="sectionForm.name" placeholder="如: 1.1 基本概念" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="sectionOpen = false">取消</el-button>
        <el-button type="primary" :loading="sectionSubmitting" @click="submitSection">保存</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="kpOpen" :title="kpForm.id ? '编辑知识点' : '新增知识点'" width="460px" border-radius="12px">
      <el-form :model="kpForm" label-position="top">
        <el-form-item label="知识点名称" required>
          <el-input v-model="kpForm.name" placeholder="如: 二叉树遍历" />
        </el-form-item>
        <el-form-item label="归属位置">
          <div class="belonging-info">
            <template v-if="selectedSectionName">
              <el-tag type="success" size="small">小节</el-tag>
              <span class="belonging-name">{{ selectedSectionName }}</span>
            </template>
            <template v-else-if="selectedChapterName">
              <el-tag type="primary" size="small">章节</el-tag>
              <span class="belonging-name">{{ selectedChapterName }}</span>
            </template>
            <template v-else>
              <el-tag type="info" size="small">课程</el-tag>
              <span class="belonging-name">直接归属于课程</span>
            </template>
          </div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="kpOpen = false">取消</el-button>
        <el-button type="primary" :loading="kpSubmitting" @click="submitKp">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.teacher-courses-container {
  padding: 0;
}

.course-list-card {
  border-radius: 12px;
  height: calc(100vh - 120px);
  overflow-y: auto;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.title-with-icon, .card-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 600;
  color: #303133;
}

.card-title-group {
  display: flex;
  align-items: center;
  gap: 6px;
}

.filter-tip {
  font-size: 12px;
  color: #909399;
  font-weight: normal;
  max-width: 120px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.empty-state, .empty-hint {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 20px 0;
  color: #909399;
  font-size: 14px;
}

.course-selector {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.course-item {
  padding: 12px 16px;
  border-radius: 8px;
  cursor: pointer;
  display: flex;
  justify-content: space-between;
  align-items: center;
  transition: all 0.3s;
  border: 1px solid transparent;
  color: #606266;
}

.course-item:hover {
  background: #f5f7fa;
  color: #409eff;
}

.course-item.active {
  background: #f0f7ff;
  color: #409eff;
  border-color: #409eff;
  font-weight: 600;
}

.management-tabs {
  border-radius: 12px;
  overflow: hidden;
  box-shadow: 0 2px 12px 0 rgba(0,0,0,0.05);
}

.structure-card {
  border-radius: 8px;
  height: 400px;
  display: flex;
  flex-direction: column;
}

.structure-card :deep(.el-card__body) {
  flex: 1;
  overflow-y: auto;
  padding: 12px;
}

.structure-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-height: 50px;
}

.structure-item {
  padding: 6px 10px;
  border-radius: 6px;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 6px;
  transition: all 0.2s;
  background: #fafafa;
  position: relative;
  font-size: 13px;
}

.structure-item:hover {
  background: #f0f7ff;
}

.structure-item.active {
  background: #409eff;
  color: white;
}

.structure-item .item-name {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  line-height: 1.5;
}

.structure-item .item-actions {
  display: flex;
  gap: 2px;
  opacity: 0;
  transition: opacity 0.2s;
  background: inherit;
  padding-left: 4px;
}

.structure-item:hover .item-actions {
  opacity: 1;
}

.structure-item.active .item-actions :deep(.el-button) {
  color: white;
}

.structure-item.kp-item {
  cursor: default;
}

.structure-item.kp-item .item-actions {
  opacity: 0;
}

.structure-item.kp-item:hover .item-actions {
  opacity: 1;
}

.tab-label {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 15px;
}

.tab-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.tab-title {
  font-weight: 600;
  font-size: 16px;
  color: #303133;
}

.actions {
  display: flex;
  gap: 12px;
  align-items: center;
}

.kp-cell, .res-cell {
  display: flex;
  align-items: center;
  gap: 8px;
  color: #606266;
}

.upload-section {
  background: #fdfdfd;
  padding: 20px;
  border-radius: 8px;
  border: 1px dashed #dcdfe6;
  margin-bottom: 24px;
}

.section-header {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 600;
  margin-bottom: 20px;
  color: #303133;
}

.upload-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-top: 10px;
}

.file-input-wrapper {
  position: relative;
  display: flex;
  align-items: center;
  gap: 12px;
}

.hidden-input {
  position: absolute;
  left: 0;
  top: 0;
  opacity: 0;
  width: 100px;
  height: 40px;
  cursor: pointer;
}

.catalog-import {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-top: 16px;
}

.batch-results {
  margin-top: 20px;
}

.no-file {
  color: #909399;
  font-size: 13px;
}

.file-name {
  color: #67c23a;
  font-size: 13px;
}

.belonging-info {
  display: flex;
  align-items: center;
  gap: 8px;
  background: #f5f7fa;
  padding: 8px 12px;
  border-radius: 6px;
  border: 1px solid #e4e7ed;
}

.belonging-name {
  font-size: 14px;
  color: #606266;
  font-weight: 500;
}
</style>
