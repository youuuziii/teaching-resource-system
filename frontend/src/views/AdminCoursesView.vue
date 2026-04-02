<script setup>
import { computed, onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import api from '../api/client'

const loading = ref(false)
const courses = ref([])
const teachers = ref([])
const assignedByCourseId = ref({})
const keyword = ref('')
const deletingId = ref(null)

const rolesOfMe = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) ? u.roles : []
  } catch {
    return []
  }
})
const isDean = computed(() => rolesOfMe.value.includes('dean') || rolesOfMe.value.includes('admin'))

const createOpen = ref(false)
const createForm = ref({ name: '', code: '', description: '' })

const assignOpen = ref(false)
const assignLoading = ref(false)
const assignCourse = ref(null)
const assignTeacherIds = ref([])

const teacherOptions = computed(() => (teachers.value || []).map((t) => ({ label: t.name, value: t.id })))

async function fetchCourses() {
  loading.value = true
  try {
    const resp = await api.get('/api/courses', { params: { q: (keyword.value || '').trim() || undefined } })
    courses.value = resp.data.items || []
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载课程失败')
    courses.value = []
  } finally {
    loading.value = false
  }
}

async function fetchTeachers() {
  try {
    const resp = await api.get('/api/teachers')
    teachers.value = resp.data.items || []
  } catch (e) {
    teachers.value = []
  }
}

async function fetchAssigned(courseId) {
  const resp = await api.get(`/api/courses/${courseId}/teachers`)
  return resp.data.items || []
}

async function refreshAssignedMap() {
  const map = {}
  const list = courses.value || []
  await Promise.all(
    list.map(async (c) => {
      try {
        map[c.id] = await fetchAssigned(c.id)
      } catch {
        map[c.id] = []
      }
    }),
  )
  assignedByCourseId.value = map
}

function openCreate() {
  createForm.value = { name: '', code: '', description: '' }
  createOpen.value = true
}

async function submitCreate() {
  const name = (createForm.value.name || '').trim()
  const code = (createForm.value.code || '').trim()
  const description = (createForm.value.description || '').trim()
  if (!name) {
    ElMessage.warning('请输入课程名称')
    return
  }
  try {
    await api.post('/api/courses', { name, code: code || undefined, description: description || undefined })
    ElMessage.success('创建成功')
    createOpen.value = false
    await fetchCourses()
    await refreshAssignedMap()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '创建失败')
  }
}

async function openAssign(row) {
  assignCourse.value = row
  assignOpen.value = true
  assignLoading.value = true
  try {
    const items = await fetchAssigned(row.id)
    assignTeacherIds.value = (items || []).map((t) => t.id)
  } catch (e) {
    assignTeacherIds.value = []
    ElMessage.error(e?.response?.data?.error?.message || '加载已分配教师失败')
  } finally {
    assignLoading.value = false
  }
}

async function submitAssign() {
  const course = assignCourse.value
  if (!course?.id) return
  assignLoading.value = true
  try {
    await api.put(`/api/courses/${course.id}/teachers`, { teacher_ids: assignTeacherIds.value || [] })
    ElMessage.success('保存成功')
    assignOpen.value = false
    const items = await fetchAssigned(course.id)
    assignedByCourseId.value = { ...(assignedByCourseId.value || {}), [course.id]: items }
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '保存失败')
  } finally {
    assignLoading.value = false
  }
}

async function removeCourse(row) {
  try {
    await ElMessageBox.confirm(
      `确认强制删除课程「${row.name}」？将同步删除该课程下的知识点、资料、课程与教师的关联关系等，且不可恢复。`,
      '强制删除确认',
      { type: 'warning', confirmButtonText: '强制删除', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  deletingId.value = row.id
  try {
    const resp = await api.delete(`/api/courses/${row.id}`, { params: { force: 1 } })
    const d = resp.data?.deleted || {}
    ElMessage.success(`删除成功（课程:${d.course || 0} 知识点:${d.knowledge_points || 0} 资料:${d.resources || 0} 关联:${d.course_teachers || 0}）`)
    await fetchCourses()
    await refreshAssignedMap()
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '删除失败')
  } finally {
    deletingId.value = null
  }
}

onMounted(async () => {
  await Promise.all([fetchCourses(), fetchTeachers()])
  await refreshAssignedMap()
})
</script>

<template>
  <el-card>
    <template #header>
      <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
        <div style="font-weight: 600">课程与分配</div>
        <el-tag v-if="!isDean" type="warning">需要教务管理员权限</el-tag>
        <el-input v-model="keyword" clearable placeholder="按名称/编码检索" style="width: 220px" @keyup.enter="async () => { await fetchCourses(); await refreshAssignedMap() }" />
        <el-button :loading="loading" @click="async () => { await fetchCourses(); await refreshAssignedMap() }">检索</el-button>
        <el-button type="primary" @click="openCreate">新增课程</el-button>
      </div>
    </template>

    <el-table :data="courses" v-loading="loading" style="width: 100%">
      <el-table-column prop="name" label="课程" min-width="180" />
      <el-table-column prop="code" label="编码" width="140" />
      <el-table-column prop="description" label="描述" min-width="220" />
      <el-table-column label="已分配教师" min-width="260">
        <template #default="{ row }">
          <span v-if="((assignedByCourseId[row.id] || []).length === 0)">-</span>
          <el-tag v-for="t in (assignedByCourseId[row.id] || [])" :key="t.id" size="small" style="margin-right: 6px; margin-bottom: 6px">
            {{ t.name }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="created_at" label="创建时间" min-width="190" />
      <el-table-column label="操作" width="220">
        <template #default="{ row }">
          <el-button size="small" type="primary" @click="openAssign(row)">分配教师</el-button>
          <el-button size="small" type="danger" :loading="deletingId === row.id" @click="removeCourse(row)">删除课程</el-button>
        </template>
      </el-table-column>
    </el-table>
  </el-card>

  <el-dialog v-model="createOpen" title="新增课程" width="520px">
    <el-form :model="createForm" label-width="80px">
      <el-form-item label="课程名称">
        <el-input v-model="createForm.name" placeholder="如 数据结构" />
      </el-form-item>
      <el-form-item label="课程编码">
        <el-input v-model="createForm.code" placeholder="可选，如 DS-001" />
      </el-form-item>
      <el-form-item label="课程描述">
        <el-input v-model="createForm.description" type="textarea" :rows="3" placeholder="可选" />
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button @click="createOpen = false">取消</el-button>
      <el-button type="primary" @click="submitCreate">创建</el-button>
    </template>
  </el-dialog>

  <el-dialog v-model="assignOpen" title="分配教师" width="560px">
    <el-form label-width="80px">
      <el-form-item label="课程">
        <el-input :model-value="assignCourse?.name || ''" disabled />
      </el-form-item>
      <el-form-item label="教师">
        <el-select v-model="assignTeacherIds" multiple filterable style="width: 100%" placeholder="选择教师" :loading="assignLoading">
          <el-option v-for="opt in teacherOptions" :key="opt.value" :label="opt.label" :value="opt.value" />
        </el-select>
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button @click="assignOpen = false">取消</el-button>
      <el-button type="primary" :loading="assignLoading" @click="submitAssign">保存</el-button>
    </template>
  </el-dialog>
</template>
