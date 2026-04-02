<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import api from '../api/client'

const roles = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) ? u.roles : []
  } catch {
    return []
  }
})
const isTeacherOnly = computed(() => roles.value.includes('teacher') && !roles.value.includes('dean') && !roles.value.includes('admin'))

const loadingCourses = ref(false)
const courses = ref([])
const courseId = ref(null)

const loadingKps = ref(false)
const knowledgePoints = ref([])

const kpOpen = ref(false)
const kpSubmitting = ref(false)
const kpForm = ref({ id: null, name: '' })

const resourceStatus = ref('all')
const loadingResources = ref(false)
const resources = ref([])

const uploadSubmitting = ref(false)
const uploadForm = ref({ title: '', description: '', knowledge_point_id: null, file: null })

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

function openCreateKp() {
  kpForm.value = { id: null, name: '' }
  kpOpen.value = true
}

function openEditKp(row) {
  kpForm.value = { id: row.id, name: row.name || '' }
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
  try {
    if (kpForm.value.id) {
      await api.patch(`/api/knowledge-points/${kpForm.value.id}`, { name })
      ElMessage.success('保存成功')
    } else {
      await api.post('/api/knowledge-points', { name, course_id: courseId.value })
      ElMessage.success('新增成功')
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
    await ElMessageBox.confirm(`确认删除知识点「${row.name}」？`, '删除确认', { type: 'warning' })
  } catch {
    return
  }
  try {
    await api.delete(`/api/knowledge-points/${row.id}`)
    ElMessage.success('删除成功')
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
  if (!isTeacherOnly.value) {
    ElMessage.error('当前角色无上传权限')
    return
  }
  if (!isNumberValue(courseId.value)) {
    ElMessage.error('请先选择课程')
    return
  }
  if (!isNumberValue(uploadForm.value.knowledge_point_id)) {
    ElMessage.warning('请选择知识点')
    return
  }
  if (!uploadForm.value.file) {
    ElMessage.warning('请选择文件')
    return
  }
  const fd = new FormData()
  fd.append('file', uploadForm.value.file)
  fd.append('title', (uploadForm.value.title || '').trim() || uploadForm.value.file.name)
  if ((uploadForm.value.description || '').trim()) fd.append('description', uploadForm.value.description.trim())
  fd.append('course_id', String(courseId.value))
  fd.append('knowledge_point_id', String(uploadForm.value.knowledge_point_id))
  uploadSubmitting.value = true
  try {
    await api.post('/api/resources/upload', fd, { headers: { 'Content-Type': 'multipart/form-data' } })
    ElMessage.success('上传成功，等待审核')
    uploadForm.value = { title: '', description: '', knowledge_point_id: null, file: null }
    await fetchMyResources()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '上传失败')
  } finally {
    uploadSubmitting.value = false
  }
}

async function removeResource(row) {
  try {
    await ElMessageBox.confirm(`确认删除资源「${row.title}」？`, '删除确认', { type: 'warning' })
  } catch {
    return
  }
  try {
    await api.delete(`/api/resources/${row.id}`)
    ElMessage.success('删除成功')
    await fetchMyResources()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '删除失败')
  }
}

async function download(item) {
  try {
    const url = `${import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:5000'}/api/resources/${item.id}/download`
    window.open(url, '_blank')
  } catch (e) {
    ElMessage.error('下载失败')
  }
}

watch(courseId, async () => {
  uploadForm.value.knowledge_point_id = null
  await Promise.all([fetchKnowledgePoints(), fetchMyResources()])
})

watch(resourceStatus, fetchMyResources)

onMounted(async () => {
  await fetchCourses()
  await Promise.all([fetchKnowledgePoints(), fetchMyResources()])
})
</script>

<template>
  <el-row :gutter="16">
    <el-col :xs="24" :md="8">
      <el-card>
        <template #header>
          <div style="display: flex; align-items: center; justify-content: space-between">
            <span>我的课程</span>
            <el-button size="small" :loading="loadingCourses" @click="fetchCourses">刷新</el-button>
          </div>
        </template>
        <el-alert v-if="!isTeacherOnly" type="warning" show-icon :closable="false">需要教师账号</el-alert>
        <el-alert v-else-if="(courses || []).length === 0" type="info" show-icon :closable="false">暂无分配课程</el-alert>
        <el-select v-else v-model="courseId" style="width: 100%" placeholder="选择课程">
          <el-option v-for="c in courses" :key="c.id" :label="c.name" :value="c.id" />
        </el-select>
      </el-card>
    </el-col>

    <el-col :xs="24" :md="16">
      <el-tabs type="border-card">
        <el-tab-pane label="知识点管理">
          <el-card>
            <template #header>
              <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
                <div style="font-weight: 600">知识点</div>
                <el-button size="small" type="primary" :disabled="!isNumberValue(courseId)" @click="openCreateKp">新增</el-button>
                <el-button size="small" :loading="loadingKps" @click="fetchKnowledgePoints">刷新</el-button>
              </div>
            </template>
            <el-table :data="knowledgePoints" v-loading="loadingKps" style="width: 100%">
              <el-table-column prop="name" label="名称" min-width="220" />
              <el-table-column label="操作" width="180">
                <template #default="{ row }">
                  <el-button size="small" @click="openEditKp(row)">编辑</el-button>
                  <el-button size="small" type="danger" @click="removeKp(row)">删除</el-button>
                </template>
              </el-table-column>
            </el-table>
          </el-card>
        </el-tab-pane>

        <el-tab-pane label="资源管理">
          <el-card style="margin-bottom: 12px">
            <template #header>
              <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
                <div style="font-weight: 600">上传资源</div>
              </div>
            </template>
            <el-form label-width="80px" @submit.prevent="submitUpload">
              <el-form-item label="标题">
                <el-input v-model="uploadForm.title" placeholder="可选，默认文件名" />
              </el-form-item>
              <el-form-item label="描述">
                <el-input v-model="uploadForm.description" type="textarea" :rows="3" placeholder="可选" />
              </el-form-item>
              <el-form-item label="知识点">
                <el-select v-model="uploadForm.knowledge_point_id" filterable placeholder="选择知识点" style="width: 100%" :disabled="!isNumberValue(courseId)">
                  <el-option v-for="k in knowledgePoints" :key="k.id" :label="k.name" :value="k.id" />
                </el-select>
              </el-form-item>
              <el-form-item label="文件">
                <input type="file" accept=".pdf,.doc,.docx" :disabled="uploadSubmitting" @change="onFileChange" />
              </el-form-item>
              <el-form-item>
                <el-button type="primary" :loading="uploadSubmitting" @click="submitUpload">上传</el-button>
              </el-form-item>
            </el-form>
          </el-card>

          <el-card>
            <template #header>
              <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
                <div style="font-weight: 600">我的资源</div>
                <el-select v-model="resourceStatus" style="width: 140px">
                  <el-option label="全部" value="all" />
                  <el-option label="待审核" value="pending" />
                  <el-option label="已通过" value="approved" />
                  <el-option label="已拒绝" value="rejected" />
                </el-select>
                <el-button size="small" :loading="loadingResources" @click="fetchMyResources">刷新</el-button>
              </div>
            </template>
            <el-table :data="resources" v-loading="loadingResources" style="width: 100%">
              <el-table-column prop="title" label="标题" min-width="220" />
              <el-table-column prop="knowledge_point" label="知识点" min-width="140" />
              <el-table-column prop="status" label="状态" width="110" />
              <el-table-column label="操作" width="220">
                <template #default="{ row }">
                  <el-button size="small" @click="download(row)">下载</el-button>
                  <el-button size="small" type="danger" @click="removeResource(row)">删除</el-button>
                </template>
              </el-table-column>
            </el-table>
          </el-card>
        </el-tab-pane>
      </el-tabs>
    </el-col>
  </el-row>

  <el-dialog v-model="kpOpen" :title="kpForm.id ? '编辑知识点' : '新增知识点'" width="520px">
    <el-form :model="kpForm" label-width="80px">
      <el-form-item label="名称">
        <el-input v-model="kpForm.name" placeholder="如 二叉树" />
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button @click="kpOpen = false">取消</el-button>
      <el-button type="primary" :loading="kpSubmitting" @click="submitKp">保存</el-button>
    </template>
  </el-dialog>
</template>
